// Fase 0 — Leitura tipada e validada de configuração via variáveis de
// ambiente. Falha rápido (na inicialização do processo) se algo
// obrigatório estiver ausente ou inválido, em vez de propagar um valor
// inconsistente para dentro do WAL/índice/segmentos.

import { availableParallelism } from 'node:os';
import { resolve } from 'node:path';

export interface Env {
  dataDir: string;
  port: number;
  workerPoolSize: number;
  maxHeapLogs: number;
  lruCacheMaxItems: number;
  flushIntervalMs: number;
}

function requireEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.trim() === '') {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function parsePositiveInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.trim() === '') {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid environment variable ${name}: expected a positive integer, got "${raw}"`);
  }
  return parsed;
}

function loadEnv(): Env {
  const dataDir = resolve(requireEnv('DATA_DIR'));
  const port = parsePositiveInt('PORT', 3000);

  // Fallback para os.availableParallelism() em vez de os.cpus().length:
  // dentro de um container com CPU limitada via cgroups (--cpus no
  // docker run / deploy.resources.limits.cpus no Compose), cpus().length
  // reporta os núcleos físicos do HOST, não a fração alocada ao container,
  // o que superdimensionaria o pool de workers.
  const workerPoolSize = parsePositiveInt('WORKER_POOL_SIZE', availableParallelism());

  const maxHeapLogs = parsePositiveInt('MAX_HEAP_LOGS', 10_000);
  const lruCacheMaxItems = parsePositiveInt('LRU_CACHE_MAX_ITEMS', 20);
  const flushIntervalMs = parsePositiveInt('FLUSH_INTERVAL_MS', 30_000);

  return { dataDir, port, workerPoolSize, maxHeapLogs, lruCacheMaxItems, flushIntervalMs };
}

export const env: Env = loadEnv();
