// Fase 6 — Thread Principal: servidor Fastify, WAL, pool de workers e
// motor de busca. Este arquivo é essencialmente "wiring": toda a lógica
// de negócio já foi implementada nas Fases 1-5.

import { randomUUID } from 'node:crypto';
import { access, readdir } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import Fastify from 'fastify';

import { env } from './config/env.js';
import { ensureWalDir, truncateWalBatch, writeWalBatch } from './wal/walWriter.js';
import { replayWal } from './wal/walReader.js';
import { readSegmentIndex } from './workers/segmentWriter.js';
import type { LogEntry, MainThreadMessage, Posting, SegmentMeta, WorkerMessage } from './types/index.js';
import { SearchEngine, type RamIndexProvider, type SegmentCatalog } from './query/searchEngine.js';
import { SegmentIndexLruCache } from './query/lruCache.js';

const currentFile = fileURLToPath(import.meta.url);
const currentDir = dirname(currentFile);
const isCompiled = currentFile.endsWith('.js');
const WORKER_ENTRY = join(currentDir, 'workers', `indexWorker.${isCompiled ? 'js' : 'ts'}`);
// Em desenvolvimento (rodando via tsx a partir de .ts), o worker_thread
// também precisa do loader tsx para conseguir importar TypeScript.
const WORKER_EXEC_ARGV = isCompiled ? [] : ['--import', 'tsx'];

// ---------------------------------------------------------------------------
// Pool de workers: distribui lotes round-robin, agrega buscas e textos
// vindos de todas as partições, e propaga confirmações de flush.
// ---------------------------------------------------------------------------

interface PendingAck {
  resolve: () => void;
  reject: (reason: unknown) => void;
}

interface PendingAggregation<T> {
  remaining: number;
  value: T;
  resolve: (value: T) => void;
}

class WorkerPool {
  private readonly workers: Worker[] = [];
  private readonly alive: boolean[] = [];
  private readonly readyPromises: Promise<void>[] = [];
  private nextWorkerIndex = 0;

  private readonly pendingAcks = new Map<string, PendingAck>();
  private readonly pendingSearches = new Map<string, PendingAggregation<Record<string, Posting[]>>>();
  private readonly pendingTexts = new Map<string, PendingAggregation<Record<string, string>>>();

  readonly onFlushed: Array<(segment: SegmentMeta, batchIds: string[]) => void> = [];

  constructor(size: number, entry: string, execArgv: string[]) {
    for (let i = 0; i < size; i++) {
      const worker = new Worker(entry, { execArgv });
      this.alive.push(false);

      this.readyPromises.push(
        new Promise<void>((resolve) => {
          const onReady = (message: WorkerMessage) => {
            if (message.type === 'READY') {
              this.alive[i] = true;
              worker.off('message', onReady);
              resolve();
            }
          };
          worker.on('message', onReady);
        }),
      );

      worker.on('message', (message: WorkerMessage) => this.handleMessage(message));
      worker.on('error', (error) => {
        this.alive[i] = false;
        console.error(`[worker ${i}] error:`, error);
      });
      worker.on('exit', (code) => {
        this.alive[i] = false;
        if (code !== 0) console.error(`[worker ${i}] exited with code ${code}`);
      });

      this.workers.push(worker);
    }
  }

  async waitUntilReady(): Promise<void> {
    await Promise.all(this.readyPromises);
  }

  isHealthy(): boolean {
    return this.alive.length > 0 && this.alive.every(Boolean);
  }

  private pickWorker(): Worker {
    const worker = this.workers[this.nextWorkerIndex];
    this.nextWorkerIndex = (this.nextWorkerIndex + 1) % this.workers.length;
    return worker;
  }

  indexBatch(entries: LogEntry[], batchId: string = randomUUID()): Promise<void> {
    const worker = this.pickWorker();
    return new Promise<void>((resolve, reject) => {
      this.pendingAcks.set(batchId, { resolve, reject });
      const message: MainThreadMessage = { type: 'INDEX_BATCH', batchId, entries };
      worker.postMessage(message);
    });
  }

  broadcastSearch(terms: string[]): Promise<Record<string, Posting[]>> {
    const requestId = randomUUID();
    return new Promise((resolve) => {
      this.pendingSearches.set(requestId, { remaining: this.workers.length, value: {}, resolve });
      const message: MainThreadMessage = { type: 'SEARCH_TERMS', requestId, terms };
      for (const worker of this.workers) worker.postMessage(message);
    });
  }

  broadcastGetTexts(docIds: string[]): Promise<Record<string, string>> {
    if (docIds.length === 0) return Promise.resolve({});
    const requestId = randomUUID();
    return new Promise((resolve) => {
      this.pendingTexts.set(requestId, { remaining: this.workers.length, value: {}, resolve });
      const message: MainThreadMessage = { type: 'GET_TEXTS', requestId, docIds };
      for (const worker of this.workers) worker.postMessage(message);
    });
  }

  private handleMessage(message: WorkerMessage): void {
    switch (message.type) {
      case 'ACK': {
        this.pendingAcks.get(message.batchId)?.resolve();
        this.pendingAcks.delete(message.batchId);
        break;
      }
      case 'INDEX_ERROR': {
        this.pendingAcks.get(message.batchId)?.reject(new Error(message.error));
        this.pendingAcks.delete(message.batchId);
        break;
      }
      case 'FLUSHED': {
        for (const callback of this.onFlushed) callback(message.segment, message.batchIds);
        break;
      }
      case 'FLUSH_ERROR': {
        console.error('Segment flush failed:', message.error);
        break;
      }
      case 'SEARCH_RESULT': {
        const pending = this.pendingSearches.get(message.requestId);
        if (!pending) break;
        for (const [term, postings] of Object.entries(message.postings)) {
          pending.value[term] = (pending.value[term] ?? []).concat(postings);
        }
        if (--pending.remaining === 0) {
          pending.resolve(pending.value);
          this.pendingSearches.delete(message.requestId);
        }
        break;
      }
      case 'TEXTS_RESULT': {
        const pending = this.pendingTexts.get(message.requestId);
        if (!pending) break;
        Object.assign(pending.value, message.texts);
        if (--pending.remaining === 0) {
          pending.resolve(pending.value);
          this.pendingTexts.delete(message.requestId);
        }
        break;
      }
      case 'READY':
        break;
    }
  }

  async shutdown(): Promise<void> {
    await Promise.all(this.workers.map((worker) => worker.terminate()));
  }
}

class WorkerPoolRamProvider implements RamIndexProvider {
  constructor(private readonly pool: WorkerPool) {}

  getPostingsForTerms(terms: string[]): Promise<Record<string, Posting[]>> {
    return this.pool.broadcastSearch(terms);
  }

  getTexts(docIds: string[]): Promise<Record<string, string>> {
    return this.pool.broadcastGetTexts(docIds);
  }
}

class InMemorySegmentCatalog implements SegmentCatalog {
  private readonly segments = new Map<string, SegmentMeta>();

  add(meta: SegmentMeta): void {
    this.segments.set(meta.segmentId, meta);
  }

  listSegments(): SegmentMeta[] {
    return [...this.segments.values()];
  }
}

/** Reconstrói o catálogo de segmentos a partir do disco na inicialização (o volume Docker sobrevive a restarts). */
async function loadExistingSegments(dataDir: string, catalog: InMemorySegmentCatalog): Promise<void> {
  const dir = join(dataDir, 'segments');
  let files: string[];
  try {
    files = await readdir(dir);
  } catch {
    return; // primeira execução: diretório de segmentos ainda não existe
  }

  for (const file of files) {
    if (!file.endsWith('.idx')) continue;
    try {
      const { meta } = await readSegmentIndex(join(dir, file));
      catalog.add(meta);
    } catch (error) {
      console.error(`Failed to load segment index ${file}:`, error);
    }
  }
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const app = Fastify({ logger: true });

  await ensureWalDir();

  const catalog = new InMemorySegmentCatalog();
  await loadExistingSegments(env.dataDir, catalog);

  const pool = new WorkerPool(env.workerPoolSize, WORKER_ENTRY, WORKER_EXEC_ARGV);
  await pool.waitUntilReady();

  pool.onFlushed.push((segment, batchIds) => {
    catalog.add(segment);
    for (const batchId of batchIds) {
      truncateWalBatch(batchId).catch((error) => {
        app.log.error(error, `Failed to truncate WAL batch ${batchId} after flush`);
      });
    }
  });

  // Crash recovery: reindexa lotes cujo flush nunca chegou a ser
  // confirmado (o servidor pode ter caído entre o fsync do WAL e o
  // flush do segmento). Reusa o batchId original para que, quando esse
  // lote finalmente for incluído num flush, o WAL correto seja truncado.
  const pendingRecords = await replayWal();
  for (const record of pendingRecords) {
    await pool.indexBatch(record.entries, record.batchId);
  }
  if (pendingRecords.length > 0) {
    app.log.info(`Recovered ${pendingRecords.length} pending WAL batch(es) on startup`);
  }

  const ramProvider = new WorkerPoolRamProvider(pool);
  const lruCache = new SegmentIndexLruCache(env.lruCacheMaxItems);
  const searchEngine = new SearchEngine(ramProvider, catalog, lruCache);

  app.post('/v1/logs', async (request, reply) => {
    const body = request.body as unknown;
    const rawItems = Array.isArray(body) ? body : (body as { logs?: unknown[] } | null)?.logs;

    if (!Array.isArray(rawItems) || rawItems.length === 0) {
      return reply.code(400).send({ error: 'Request body must be a non-empty array of logs, or { "logs": [...] }.' });
    }

    const entries: LogEntry[] = rawItems.map((item) => ({
      id: randomUUID(),
      text: typeof item === 'string' ? item : JSON.stringify(item),
      timestamp: Date.now(),
    }));

    const batchId = randomUUID();
    try {
      // fsync ocorre dentro de writeWalBatch, ANTES desta linha retornar:
      // só respondemos 202 depois que os dados estão fisicamente no disco.
      await writeWalBatch(batchId, entries);
    } catch (error) {
      request.log.error(error, 'Failed to persist WAL batch');
      return reply.code(500).send({ error: 'Failed to persist logs durably.' });
    }

    // A indexação em memória acontece depois da confirmação de
    // durabilidade e não bloqueia a resposta HTTP: se o worker falhar,
    // o replay do WAL na próxima inicialização recupera o lote.
    pool.indexBatch(entries, batchId).catch((error) => {
      request.log.error(error, `Failed to index batch ${batchId}`);
    });

    return reply.code(202).send({ accepted: entries.length, batchId });
  });

  app.get('/v1/search', async (request, reply) => {
    const { q, limit, offset } = request.query as { q?: string; limit?: string; offset?: string };
    if (!q || q.trim() === '') {
      return reply.code(400).send({ error: 'Query parameter "q" is required.' });
    }

    const result = await searchEngine.search(q, {
      limit: limit ? Number.parseInt(limit, 10) : undefined,
      offset: offset ? Number.parseInt(offset, 10) : undefined,
    });
    return reply.send(result);
  });

  app.get('/healthz', async (_request, reply) => {
    const workersHealthy = pool.isHealthy();
    let walHealthy = true;
    try {
      await access(join(env.dataDir, 'wal'));
    } catch {
      walHealthy = false;
    }

    const healthy = workersHealthy && walHealthy;
    return reply.code(healthy ? 200 : 503).send({
      status: healthy ? 'ok' : 'degraded',
      workers: workersHealthy,
      wal: walHealthy,
    });
  });

  const shutdown = async (signal: string): Promise<void> => {
    app.log.info(`Received ${signal}, shutting down gracefully`);
    await app.close();
    await pool.shutdown();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  await app.listen({ port: env.port, host: '0.0.0.0' });
}

main().catch((error) => {
  console.error('Fatal error during startup:', error);
  process.exit(1);
});
