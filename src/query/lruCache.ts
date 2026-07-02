// Fase 5 — Cache LRU de índices de segmentos (.idx) carregados do disco.

import type { SegmentMeta } from '../types/index.js';
import { readSegmentIndex, type SerializedSegmentIndex } from '../workers/segmentWriter.js';

/**
 * Mantém em memória apenas os índices `.idx` dos segmentos mais recentes
 * ou frequentemente buscados, evitando estouro de RAM ao pesquisar sobre
 * um histórico grande de segmentos. `Map` preserva ordem de inserção: ao
 * reinserir uma chave já existente (`delete` + `set`), ela vira a "mais
 * recentemente usada"; o candidato a descarte é sempre a primeira chave
 * do Map (a menos recentemente usada).
 *
 * Os arquivos `.idx` são metadados compactos (KBs), por isso lemos o
 * arquivo inteiro de uma vez em vez de fazer streaming/parsing incremental
 * de JSON — o streaming de verdade acontece na leitura pontual das linhas
 * de log dentro dos arquivos `.data`, potencialmente muito maiores, via
 * offset+length exatos (ver searchEngine.ts).
 */
export class SegmentIndexLruCache {
  private readonly maxItems: number;
  private readonly cache = new Map<string, SerializedSegmentIndex>();

  constructor(maxItems: number) {
    if (maxItems <= 0) {
      throw new Error('SegmentIndexLruCache maxItems must be a positive integer');
    }
    this.maxItems = maxItems;
  }

  async get(segment: SegmentMeta): Promise<SerializedSegmentIndex> {
    const cached = this.cache.get(segment.segmentId);
    if (cached) {
      this.cache.delete(segment.segmentId);
      this.cache.set(segment.segmentId, cached);
      return cached;
    }

    const loaded = await readSegmentIndex(segment.idxFile);
    this.cache.set(segment.segmentId, loaded);
    this.evictIfNeeded();
    return loaded;
  }

  private evictIfNeeded(): void {
    while (this.cache.size > this.maxItems) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey === undefined) break;
      this.cache.delete(oldestKey);
    }
  }

  get size(): number {
    return this.cache.size;
  }
}
