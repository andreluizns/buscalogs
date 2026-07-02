// Fase 4 — Entry point de worker_thread: conecta InvertedIndex (Fase 2) e
// segmentWriter (Fase 3), e decide a política de gatilho de flush.

import { parentPort } from 'node:worker_threads';
import { InvertedIndex } from './invertedIndex.js';
import { flushSegment } from './segmentWriter.js';
import { env } from '../config/env.js';
import type { MainThreadMessage, WorkerMessage } from '../types/index.js';

if (!parentPort) {
  throw new Error('indexWorker.ts must be run as a worker_thread (parentPort is null)');
}

const port = parentPort;

let activeIndex = new InvertedIndex();
let pendingBatchIds: string[] = [];
let flushTimer: NodeJS.Timeout | undefined;

function send(message: WorkerMessage): void {
  port.postMessage(message);
}

function clearFlushTimer(): void {
  if (flushTimer) {
    clearTimeout(flushTimer);
    flushTimer = undefined;
  }
}

function scheduleFlushTimer(): void {
  clearFlushTimer();
  flushTimer = setTimeout(() => {
    void triggerFlush();
  }, env.flushIntervalMs);
  flushTimer.unref();
}

async function triggerFlush(): Promise<void> {
  if (activeIndex.size === 0) {
    scheduleFlushTimer();
    return;
  }

  const indexToFlush = activeIndex;
  const batchIdsToTruncate = pendingBatchIds;
  activeIndex = new InvertedIndex();
  pendingBatchIds = [];

  try {
    const segment = await flushSegment(indexToFlush, env.dataDir);
    send({ type: 'FLUSHED', segment, batchIds: batchIdsToTruncate });
  } catch (error) {
    send({ type: 'FLUSH_ERROR', error: error instanceof Error ? error.message : String(error) });
  } finally {
    scheduleFlushTimer();
  }
}

async function handleMessage(message: MainThreadMessage): Promise<void> {
  switch (message.type) {
    case 'INDEX_BATCH': {
      try {
        for (const entry of message.entries) {
          activeIndex.addDocument(entry);
        }
        pendingBatchIds.push(message.batchId);
        send({ type: 'ACK', batchId: message.batchId });

        if (activeIndex.size >= env.maxHeapLogs) {
          await triggerFlush();
        }
      } catch (error) {
        send({ type: 'INDEX_ERROR', batchId: message.batchId, error: error instanceof Error ? error.message : String(error) });
      }
      break;
    }

    case 'SEARCH_TERMS': {
      const postings: Record<string, ReturnType<InvertedIndex['getPostings']>> = {};
      for (const term of message.terms) {
        postings[term] = activeIndex.getPostings(term);
      }
      send({ type: 'SEARCH_RESULT', requestId: message.requestId, postings });
      break;
    }

    case 'GET_TEXTS': {
      const texts: Record<string, string> = {};
      for (const docId of message.docIds) {
        const doc = activeIndex.getDocument(docId);
        if (doc) texts[docId] = doc.text;
      }
      send({ type: 'TEXTS_RESULT', requestId: message.requestId, texts });
      break;
    }

    case 'FLUSH': {
      await triggerFlush();
      break;
    }

    case 'SHUTDOWN': {
      clearFlushTimer();
      port.close();
      break;
    }
  }
}

port.on('message', (message: MainThreadMessage) => {
  handleMessage(message).catch((error) => {
    // Erro inesperado fora dos try/catch específicos de cada case: reporta
    // sem derrubar o worker silenciosamente.
    send({ type: 'FLUSH_ERROR', error: error instanceof Error ? error.message : String(error) });
  });
});

scheduleFlushTimer();
send({ type: 'READY' });
