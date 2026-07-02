# Log Engine — Motor de Busca de Logs Distribuídos

Motor de busca de logs full-text, estilo Elasticsearch/Loki simplificado, escrito em **TypeScript** puro sobre **Node.js**, sem nenhum banco de dados externo. Toda a indexação, persistência e busca acontecem dentro do próprio processo Node, usando o filesystem como camada de armazenamento durável.

## Para que serve

É um serviço HTTP para:

1. **Ingerir logs em lote** (`POST /v1/logs`) com garantia de durabilidade real (Write-Ahead Log com `fsync` físico) antes de confirmar o recebimento ao cliente.
2. **Buscar logs por texto** (`GET /v1/search?q=...`) combinando dados ainda em memória (recém-recebidos) com dados já persistidos em disco, usando índice invertido e interseção de termos (lógica AND).
3. Fazer tudo isso **sem depender de Elasticsearch, Postgres, Redis ou qualquer serviço externo** — útil como motor de busca embarcado para sistemas de médio porte, laboratório de estudo de estruturas de dados de busca, ou como base para evoluir para algo maior.

## Estrutura de pastas

```
buscalogs/
├── docker/
│   └── Dockerfile               # Build multi-stage (builder -> runtime enxuto e não-root)
├── docker-compose.yml           # Orquestração de produção (volume nomeado, healthcheck)
├── docker-compose.override.yml  # Sobrepõe automaticamente em dev: hot-reload com tsx watch
├── .dockerignore
├── .env.example                 # Todas as variáveis de ambiente suportadas
├── .gitignore
├── package.json
├── tsconfig.json
├── src/
│   ├── server.ts                # Thread principal: Fastify, pool de workers, wiring geral
│   ├── config/
│   │   └── env.ts                # Leitura tipada e validada de variáveis de ambiente
│   ├── types/
│   │   └── index.ts              # Contrato único de tipos (LogEntry, Posting, SegmentMeta, protocolo de mensagens)
│   ├── wal/
│   │   ├── walWriter.ts          # Escrita sequencial + fsync + rename atômico
│   │   └── walReader.ts          # Replay do WAL para recuperação de falhas
│   ├── workers/
│   │   ├── indexWorker.ts        # Entry point de cada worker_thread (indexação + gatilho de flush)
│   │   ├── invertedIndex.ts      # Estrutura de índice invertido em memória (Map, tokenização, stop words)
│   │   └── segmentWriter.ts      # Flush para arquivos imutáveis .data/.idx com offsets em bytes
│   └── query/
│       ├── searchEngine.ts       # Orquestra busca em RAM + segmentos em disco
│       ├── lruCache.ts           # Cache LRU de índices .idx carregados do disco
│       └── intersect.ts          # Interseção eficiente de postings lists (AND lógico)
├── data/                         # Volume de dados: WAL + segmentos (criado em runtime, git-ignorado)
│   ├── wal/
│   └── segments/
├── buscalogs.md                  # Índice dos prompts que guiaram o desenvolvimento faseado (git-ignorado)
└── prompts/                      # Prompts fase-a-fase usados na geração do código (git-ignorado)
```

> `buscalogs.md` e `prompts/` documentam o processo de desenvolvimento (prompts usados fase a fase para evitar alucinação de IA), não fazem parte do software em si — por isso estão fora do controle de versão e fora da imagem Docker.

## Arquitetura

### Visão geral do fluxo

```
Cliente HTTP
   │  POST /v1/logs
   ▼
Fastify (thread principal)
   │  1. grava no WAL + fsync (durabilidade física)
   │  2. responde 202 Accepted
   │  3. despacha o lote para um worker (round-robin)
   ▼
Worker Thread (indexWorker.ts)
   │  indexa em memória (InvertedIndex — Map de termo -> postings)
   │  ao atingir MAX_HEAP_LOGS ou FLUSH_INTERVAL_MS:
   ▼
Flush (segmentWriter.ts)
   │  grava par imutável seg_<id>.data (texto bruto) + seg_<id>.idx (índice + offsets)
   │  escrita atômica (tmp + rename) — segura mesmo sobre volume Docker
   ▼
Thread principal: recebe confirmação (FLUSHED) → trunca o WAL do(s) lote(s) incluído(s)
```

```
Cliente HTTP
   │  GET /v1/search?q=erro+banco
   ▼
SearchEngine (query/searchEngine.ts)
   ├─ consulta o índice ativo em RAM (via broadcast aos workers)
   └─ consulta segmentos em disco (via cache LRU de .idx)
        │  para cada termo, obtém a postings list
        │  interseção AND entre os termos (intersect.ts)
        │  para cada docId resultante: fs.read(offset, length) direto no .data
   ▼
Resposta paginada com os hits (texto original + origem: RAM ou segmentId)
```

### Por que cada peça existe

| Componente | Decisão | Motivo |
|---|---|---|
| **WAL com `fsync`** | Grava e sincroniza fisicamente antes de responder `202` | Sem isso, um log "confirmado" ao cliente poderia sumir numa queda de energia — o SO mantém o `write()` em cache de página até o `fsync` forçar a ida ao disco |
| **`worker_threads`** | Indexação roda fora da thread principal | Tokenização/indexação é CPU-bound; rodar na mesma thread do Fastify bloquearia o event loop e derrubaria o throughput de requisições HTTP |
| **Índice invertido em `Map`** | Estrutura nativa, sem dependência externa | Suficiente para o volume de dados de um índice ativo em RAM; simples de raciocinar e serializar |
| **Segmentos imutáveis (`.data` + `.idx`)** | Par de arquivos gêmeos, nunca modificados após o flush | Imutabilidade elimina classes inteiras de bugs de concorrência/corrupção; simplifica cache (dado nunca fica stale) |
| **Offset + length em bytes** | Ponteiro direto para `fs.read` | Evita varredura linear do `.data` a cada busca — acesso O(1) à linha exata |
| **Cache LRU de `.idx`** | Mantém em RAM só os segmentos mais usados/recentes | Sem isso, buscar sobre um histórico grande estouraria a memória; segmentos frios são lidos sob demanda e descartados |
| **Escrita atômica (tmp + rename)** | Nunca escreve direto no nome final | Sobre volumes Docker (bind mount/volume nomeado), um crash no meio de um `write()` direto deixaria arquivos corrompidos; rename é atômico no mesmo diretório |
| **Fastify** | Framework HTTP | Overhead menor que Express, suporte nativo a JSON Schema e validação, bom encaixe com TypeScript |

## Trade-offs de tecnologia

### Node.js + TypeScript vs. stack com banco de dados dedicado (Elasticsearch/Loki reais)
- **Ganho**: zero dependências externas, deploy trivial (um único container), controle total sobre o formato de armazenamento e sobre o custo de cada operação.
- **Custo**: sem clustering/sharding entre múltiplas máquinas, sem replicação, sem tolerância a falha de disco — é um motor de busca *de processo único*, não um sistema distribuído de verdade apesar do nome. Adequado para volumes pequenos/médios (um único host), não para substituir Elasticsearch em escala.

### `worker_threads` vs. processos separados (`child_process`) ou cluster
- **Ganho**: memória compartilhada mais barata de coordenar via `postMessage`, sem overhead de IPC entre processos do SO, startup mais rápido.
- **Custo**: todos os workers competem pelo mesmo heap/limite de memória do processo Node; um crash do processo principal derruba todos os workers juntos (não há isolamento de falha entre eles).

### `Map` em memória vs. estrutura persistente tipo LSM-tree/B-tree desde o início
- **Ganho**: implementação simples, rápida de entender e depurar; performance de leitura/escrita excelente enquanto cabe em RAM.
- **Custo**: o índice ativo é limitado pela RAM disponível — daí a necessidade do flush por `MAX_HEAP_LOGS`. Não há compactação de segmentos (merge de segmentos antigos), então o número de arquivos `.idx`/`.data` cresce indefinidamente e cada busca precisa varrer todos os segmentos existentes.

### JSON como formato de serialização do `.idx` vs. formato binário customizado
- **Ganho**: simples de depurar (dá pra abrir o arquivo e ler), sem necessidade de escrever um (de)serializador binário próprio.
- **Custo**: mais bytes em disco e mais CPU para parse do que um formato binário compacto (ex: um formato tipo FST/trie serializado) — aceitável para o tamanho de um `.idx` (metadados + postings), mas seria o primeiro ponto a otimizar num cenário de segmentos muito grandes.

### Fastify 4.x vs. 5.x
- Fixado em `^4.29.1` (última patch da série 4) em vez da major 5, que corrige o único CVE "high" remanescente do `npm audit`. Evita uma migração breaking não solicitada; é a troca mais simples para elevar a segurança caso o projeto avance para produção.

## Como rodar

### Pré-requisitos
- Node.js >= 20 (para rodar localmente sem Docker)
- Docker + Docker Compose (para rodar containerizado)

### Variáveis de ambiente

Copie `.env.example` para `.env` e ajuste se necessário:

```bash
cp .env.example .env
```

| Variável | Obrigatória | Padrão | Descrição |
|---|---|---|---|
| `DATA_DIR` | Sim | — | Diretório onde WAL e segmentos são persistidos |
| `PORT` | Não | `3000` | Porta HTTP do servidor |
| `WORKER_POOL_SIZE` | Não | `os.availableParallelism()` | Quantidade de worker_threads |
| `MAX_HEAP_LOGS` | Não | `10000` | Documentos em RAM antes do flush automático |
| `LRU_CACHE_MAX_ITEMS` | Não | `20` | Segmentos `.idx` mantidos em cache simultaneamente |
| `FLUSH_INTERVAL_MS` | Não | `30000` | Intervalo máximo entre flushes, mesmo sem atingir `MAX_HEAP_LOGS` |

### Opção 1 — Local (Node.js direto)

```bash
npm install
npm run build      # compila TypeScript -> dist/
DATA_DIR=./data npm start
```

Ou em modo desenvolvimento, com hot-reload:

```bash
npm install
DATA_DIR=./data npm run dev
```

### Opção 2 — Docker (produção)

```bash
docker compose -f docker-compose.yml up --build
```

Sobe a imagem final (multi-stage, usuário não-root, healthcheck em `/healthz`), com os dados persistidos no volume nomeado `log-data`.

### Opção 3 — Docker (desenvolvimento com hot-reload)

```bash
docker compose up --build
```

Sem especificar `-f`, o Compose aplica automaticamente o `docker-compose.override.yml` por cima do arquivo de produção, montando `src/` do host dentro do container e rodando via `tsx watch` — qualquer alteração no código reinicia o servidor sozinho.

### Testando a API

```bash
# Ingerir logs
curl -X POST http://localhost:3000/v1/logs \
  -H "Content-Type: application/json" \
  -d '["erro de conexao com banco de dados", "usuario autenticado com sucesso"]'

# Buscar (interseção AND entre termos)
curl "http://localhost:3000/v1/search?q=erro+banco"

# Healthcheck (WAL acessível + workers vivos)
curl http://localhost:3000/healthz
```

### Scripts disponíveis

| Comando | Descrição |
|---|---|
| `npm run build` | Compila TypeScript para `dist/` |
| `npm start` | Roda a versão compilada (`dist/server.js`) |
| `npm run dev` | Roda via `tsx watch` com hot-reload, direto do TypeScript |
| `npm run typecheck` | Só verifica tipos (`tsc --noEmit`), sem gerar output |
