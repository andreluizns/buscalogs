// Fase 5 — Motor de busca: combina índice ativo em RAM (via provider
// injetado, que na Fase 6 fala com o pool de workers) e segmentos em
// disco (via cache LRU), usando interseção de postings e offsets de bytes
// para leitura direta das linhas de log.

import { open } from 'node:fs/promises';
import type { Posting, SegmentMeta } from '../types/index.js';
import { intersectDocIds } from './intersect.js';
import type { SegmentIndexLruCache } from './lruCache.js';

export interface RamIndexProvider {
  getPostingsForTerms(terms: string[]): Promise<Record<string, Posting[]>>;
  getTexts(docIds: string[]): Promise<Record<string, string>>;
}

export interface SegmentCatalog {
  listSegments(): SegmentMeta[];
}

export interface SearchHit {
  docId: string;
  text: string;
  /** 'ram' para documentos ainda não flushados, ou o segmentId de origem. */
  source: 'ram' | string;
}

export interface SearchResult {
  hits: SearchHit[];
  total: number;
  limit: number;
  offset: number;
}

/**
 * Lê exatamente os bytes [offset, offset+length) de um arquivo .data,
 * usando o ponteiro calculado na Fase 3 — acesso direto via fs.read,
 * sem varredura linear do arquivo.
 */
async function readSliceFromDataFile(dataFile: string, offset: number, length: number): Promise<string> {
  const handle = await open(dataFile, 'r');
  try {
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, offset);
    return buffer.toString('utf8');
  } finally {
    await handle.close();
  }
}

export class SearchEngine {
  constructor(
    private readonly ramProvider: RamIndexProvider,
    private readonly segmentCatalog: SegmentCatalog,
    private readonly lruCache: SegmentIndexLruCache,
  ) {}

  async search(rawQuery: string, opts: { limit?: number; offset?: number } = {}): Promise<SearchResult> {
    const limit = opts.limit && opts.limit > 0 ? opts.limit : 20;
    const offset = opts.offset && opts.offset > 0 ? opts.offset : 0;

    const terms = rawQuery
      .toLowerCase()
      .split(/\s+/)
      .map((t) => t.trim())
      .filter(Boolean);

    if (terms.length === 0) {
      return { hits: [], total: 0, limit, offset };
    }

    const [ramHits, segmentHits] = await Promise.all([this.searchRam(terms), this.searchSegments(terms)]);

    const all = [...ramHits, ...segmentHits];
    const page = all.slice(offset, offset + limit);

    return { hits: page, total: all.length, limit, offset };
  }

  private async searchRam(terms: string[]): Promise<SearchHit[]> {
    const postingsByTerm = await this.ramProvider.getPostingsForTerms(terms);
    const docIdLists = terms.map((term) => (postingsByTerm[term] ?? []).map((p) => p.docId));
    const matchedIds = intersectDocIds(docIdLists);
    if (matchedIds.length === 0) return [];

    const texts = await this.ramProvider.getTexts(matchedIds);
    return matchedIds
      .filter((id) => texts[id] !== undefined)
      .map((id) => ({ docId: id, text: texts[id], source: 'ram' as const }));
  }

  private async searchSegments(terms: string[]): Promise<SearchHit[]> {
    const hits: SearchHit[] = [];

    for (const segment of this.segmentCatalog.listSegments()) {
      const segmentIndex = await this.lruCache.get(segment);
      const docIdLists = terms.map((term) => (segmentIndex.postings[term] ?? []).map((p) => p.docId));
      if (docIdLists.some((list) => list.length === 0)) continue; // termo ausente neste segmento

      const matchedIds = intersectDocIds(docIdLists);
      if (matchedIds.length === 0) continue;

      const locationByDocId = new Map(segment.documents.map((d) => [d.docId, d]));
      for (const docId of matchedIds) {
        const location = locationByDocId.get(docId);
        if (!location) continue;
        const text = await readSliceFromDataFile(segment.dataFile, location.offset, location.length);
        hits.push({ docId, text, source: segment.segmentId });
      }
    }

    return hits;
  }
}
