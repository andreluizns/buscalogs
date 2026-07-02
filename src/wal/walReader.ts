// Fase 1 — Write-Ahead Log: replay para recuperação de falhas.

import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { env } from '../config/env.js';
import { ensureWalDir } from './walWriter.js';
import type { WalRecord } from './walWriter.js';

function walDir(): string {
  return join(env.dataDir, 'wal');
}

/**
 * Relê todos os lotes ainda presentes no diretório do WAL (ou seja, que
 * não foram truncados após um flush confirmado). Usado na inicialização
 * do processo para reindexar logs que já foram confirmados ao cliente
 * HTTP (202) mas cujo flush para segmento ainda não tinha ocorrido
 * quando o processo foi encerrado/crashou.
 */
export async function replayWal(): Promise<WalRecord[]> {
  await ensureWalDir();
  const dir = walDir();
  const files = await readdir(dir);
  const records: WalRecord[] = [];

  for (const file of files) {
    // Ignora .tmp remanescentes: só um arquivo já renomeado para .wal
    // passou pelo fsync + rename atômico da Fase 1 e é considerado durável.
    if (!file.endsWith('.wal')) continue;

    const raw = await readFile(join(dir, file), 'utf8');
    try {
      records.push(JSON.parse(raw) as WalRecord);
    } catch {
      // Um .wal só existe após rename atômico pós-fsync bem-sucedido;
      // conteúdo inválido aqui indica corrupção do filesystem, não uma
      // escrita em andamento. Ignoramos para não travar o boot do processo.
    }
  }

  records.sort((a, b) => a.writtenAt - b.writtenAt);
  return records;
}
