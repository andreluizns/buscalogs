// Fase 1 — Write-Ahead Log: escrita sequencial e durável.

import { mkdir, open, rename, rm } from 'node:fs/promises';
import { join } from 'node:path';
import type { LogEntry } from '../types/index.js';
import { env } from '../config/env.js';

export interface WalRecord {
  batchId: string;
  entries: LogEntry[];
  writtenAt: number;
}

function walDir(): string {
  return join(env.dataDir, 'wal');
}

function walPath(batchId: string): string {
  return join(walDir(), `${batchId}.wal`);
}

export async function ensureWalDir(): Promise<void> {
  await mkdir(walDir(), { recursive: true });
}

/**
 * Grava um lote de logs de forma sequencial e durável em um arquivo
 * próprio do lote. `fsync` (FileHandle.sync) é chamado explicitamente
 * ANTES do rename atômico e antes da promise resolver: essa é a garantia
 * física de "os bytes estão no disco, sobrevivem a queda de energia" que
 * permite ao servidor (Fase 6) responder 202 Accepted com segurança.
 * Sem o fsync, o SO poderia manter os bytes apenas em cache de página,
 * perdidos numa falha elétrica mesmo após o write() retornar.
 */
export async function writeWalBatch(batchId: string, entries: LogEntry[]): Promise<void> {
  await ensureWalDir();

  const record: WalRecord = { batchId, entries, writtenAt: Date.now() };
  const payload = JSON.stringify(record);
  const finalPath = walPath(batchId);
  const tmpPath = `${finalPath}.tmp`;

  const fileHandle = await open(tmpPath, 'w');
  try {
    await fileHandle.writeFile(payload, 'utf8');
    await fileHandle.sync();
  } finally {
    await fileHandle.close();
  }

  // Escreve em arquivo temporário e só então renomeia: sobre um volume
  // Docker (bind mount ou volume nomeado), um crash no meio de um write()
  // direto no arquivo final poderia deixar um WAL parcialmente gravado.
  // O rename é atômico dentro do mesmo diretório.
  await rename(tmpPath, finalPath);

  // fsync do diretório: garante que a entrada de diretório criada pelo
  // rename também está persistida, não apenas o conteúdo do arquivo.
  const dirHandle = await open(walDir(), 'r');
  try {
    await dirHandle.sync();
  } finally {
    await dirHandle.close();
  }
}

/**
 * Remove com segurança o arquivo WAL de um lote. Só deve ser chamado
 * depois que o worker confirmar (mensagem FLUSHED) que os documentos
 * daquele lote já estão duráveis em um segmento .idx/.data imutável.
 */
export async function truncateWalBatch(batchId: string): Promise<void> {
  await rm(walPath(batchId), { force: true });
}
