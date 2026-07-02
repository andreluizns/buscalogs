// Fase 2 — Índice Invertido em memória (100% RAM, sem I/O de disco).

import type { LogEntry, Posting } from '../types/index.js';

const STOP_WORDS = new Set([
  'o', 'a', 'os', 'as', 'de', 'da', 'do', 'das', 'dos', 'em', 'no', 'na',
  'nos', 'nas', 'um', 'uma', 'uns', 'umas', 'e', 'ou', 'que', 'para',
  'por', 'com', 'sem', 'se', 'ao', 'aos', 'à', 'às', 'é', 'foi', 'ser',
]);

// Cobre letras ASCII, dígitos e as vogais/consoantes acentuadas mais comuns
// em português (faixa Unicode à-ú, que inclui ç e ñ).
const TOKEN_REGEX = /[a-zà-ú0-9]+/gi;

/**
 * Tokeniza o texto e retorna, para cada termo normalizado (lowercase, sem
 * stop words), a lista de posições em que ele ocorre. A posição é o índice
 * ordinal do token na sequência completa (contando também os tokens
 * removidos como stop words), preservando distância real entre termos
 * para futuras buscas de frase exata.
 */
function tokenizeWithPositions(text: string): Map<string, number[]> {
  const positionsByTerm = new Map<string, number[]>();
  TOKEN_REGEX.lastIndex = 0;
  let match: RegExpExecArray | null;
  let position = 0;

  while ((match = TOKEN_REGEX.exec(text)) !== null) {
    const term = match[0].toLowerCase();
    if (!STOP_WORDS.has(term)) {
      const existing = positionsByTerm.get(term);
      if (existing) {
        existing.push(position);
      } else {
        positionsByTerm.set(term, [position]);
      }
    }
    position++;
  }

  return positionsByTerm;
}

export class InvertedIndex {
  private readonly postingsByTerm = new Map<string, Posting[]>();
  private readonly documents = new Map<string, LogEntry>();

  addDocument(entry: LogEntry): void {
    if (this.documents.has(entry.id)) {
      throw new Error(`Document with id "${entry.id}" is already indexed`);
    }
    this.documents.set(entry.id, entry);

    const positionsByTerm = tokenizeWithPositions(entry.text);
    for (const [term, positions] of positionsByTerm) {
      const posting: Posting = { docId: entry.id, positions };
      const existing = this.postingsByTerm.get(term);
      if (existing) {
        existing.push(posting);
      } else {
        this.postingsByTerm.set(term, [posting]);
      }
    }
  }

  getPostings(term: string): Posting[] {
    return this.postingsByTerm.get(term.toLowerCase()) ?? [];
  }

  getDocument(docId: string): LogEntry | undefined {
    return this.documents.get(docId);
  }

  documentsInInsertionOrder(): LogEntry[] {
    return Array.from(this.documents.values());
  }

  /** Todos os pares (termo, postings) — usado pelo Segment Writer (Fase 3) para serializar o índice completo. */
  entries(): IterableIterator<[string, Posting[]]> {
    return this.postingsByTerm.entries();
  }

  get size(): number {
    return this.documents.size;
  }
}
