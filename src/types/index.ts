// Fase 0 — Contrato único de tipos. Nenhum outro arquivo deve redefinir
// estas estruturas; todas as fases seguintes importam daqui.

export interface LogEntry {
  id: string;
  text: string;
  timestamp: number;
}

export interface Posting {
  docId: string;
  /** Posições (índice do token na sequência tokenizada) em que o termo aparece. */
  positions: number[];
}

export interface DocumentLocation {
  docId: string;
  /** Deslocamento em bytes do início do texto dentro do arquivo .data. */
  offset: number;
  /** Comprimento em bytes do texto dentro do arquivo .data. */
  length: number;
}

export interface SegmentMeta {
  segmentId: string;
  createdAt: number;
  dataFile: string;
  idxFile: string;
  documents: DocumentLocation[];
}

/** Mensagens enviadas da Thread Principal para um Worker. */
export type MainThreadMessage =
  | { type: 'INDEX_BATCH'; batchId: string; entries: LogEntry[] }
  | { type: 'SEARCH_TERMS'; requestId: string; terms: string[] }
  | { type: 'GET_TEXTS'; requestId: string; docIds: string[] }
  | { type: 'FLUSH' }
  | { type: 'SHUTDOWN' };

/** Mensagens enviadas de um Worker para a Thread Principal. */
export type WorkerMessage =
  | { type: 'READY' }
  | { type: 'ACK'; batchId: string }
  | { type: 'INDEX_ERROR'; batchId: string; error: string }
  | { type: 'FLUSHED'; segment: SegmentMeta; batchIds: string[] }
  | { type: 'FLUSH_ERROR'; error: string }
  | { type: 'SEARCH_RESULT'; requestId: string; postings: Record<string, Posting[]> }
  | { type: 'TEXTS_RESULT'; requestId: string; texts: Record<string, string> };
