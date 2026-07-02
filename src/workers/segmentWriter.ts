// Fase 3 — Flush do índice ativo para dois arquivos imutáveis gêmeos
// (seg_<id>.data + seg_<id>.idx), com offsets de bytes precisos.

import { mkdir, open, readFile, rename } from 'node:fs/promises';
import { join } from 'node:path';
import type { InvertedIndex } from './invertedIndex.js';
import type { DocumentLocation, Posting, SegmentMeta } from '../types/index.js';

export interface SerializedSegmentIndex {
  meta: SegmentMeta;
  postings: Record<string, Posting[]>;
}

function segmentsDir(dataDir: string): string {
  return join(dataDir, 'segments');
}

export async function ensureSegmentsDir(dataDir: string): Promise<void> {
  await mkdir(segmentsDir(dataDir), { recursive: true });
}

/**
 * Persiste o InvertedIndex ativo como um par imutável de arquivos.
 * O offset de cada documento é calculado em BYTES via Buffer.byteLength
 * (não .length de string): caracteres multi-byte em UTF-8 (acentos, por
 * exemplo) tornariam um offset baseado em "caracteres" incorreto para
 * `fs.read`, que opera sobre bytes.
 */
export async function flushSegment(index: InvertedIndex, dataDir: string): Promise<SegmentMeta> {
  await ensureSegmentsDir(dataDir);
  const dir = segmentsDir(dataDir);
  const segmentId = `seg_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const dataFile = join(dir, `${segmentId}.data`);
  const idxFile = join(dir, `${segmentId}.idx`);
  const dataTmp = `${dataFile}.tmp`;
  const idxTmp = `${idxFile}.tmp`;

  const documents: DocumentLocation[] = [];

  const dataHandle = await open(dataTmp, 'w');
  try {
    let offset = 0;
    for (const entry of index.documentsInInsertionOrder()) {
      const buffer = Buffer.from(entry.text, 'utf8');
      await dataHandle.write(buffer, 0, buffer.length, offset);
      documents.push({ docId: entry.id, offset, length: buffer.length });
      offset += buffer.length;
    }
    await dataHandle.sync();
  } finally {
    await dataHandle.close();
  }

  const meta: SegmentMeta = {
    segmentId,
    createdAt: Date.now(),
    dataFile,
    idxFile,
    documents,
  };

  const postings: Record<string, Posting[]> = {};
  for (const [term, list] of index.entries()) {
    postings[term] = list;
  }

  const serialized: SerializedSegmentIndex = { meta, postings };
  const idxHandle = await open(idxTmp, 'w');
  try {
    await idxHandle.writeFile(JSON.stringify(serialized), 'utf8');
    await idxHandle.sync();
  } finally {
    await idxHandle.close();
  }

  // Só depois que AMBOS os renames ocorrerem o segmento é considerado
  // "existente" por qualquer leitor: nenhuma busca pode ver um .data sem
  // o .idx correspondente (ou vice-versa) em caso de crash entre os dois.
  await rename(dataTmp, dataFile);
  await rename(idxTmp, idxFile);

  const dirHandle = await open(dir, 'r');
  try {
    await dirHandle.sync();
  } finally {
    await dirHandle.close();
  }

  return meta;
}

/**
 * Lê um arquivo .idx do disco e desserializa seu conteúdo. Centralizado
 * aqui (o mesmo módulo que define o formato de escrita) para que a Fase 5
 * (busca) e a Fase 6 (catálogo de segmentos no boot) nunca precisem
 * reimplementar/adivinhar o formato de serialização.
 */
export async function readSegmentIndex(idxFile: string): Promise<SerializedSegmentIndex> {
  const raw = await readFile(idxFile, 'utf8');
  return JSON.parse(raw) as SerializedSegmentIndex;
}
