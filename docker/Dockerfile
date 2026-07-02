# syntax=docker/dockerfile:1

# ---- Stage 1: builder --------------------------------------------------
# Instala devDependencies e compila TypeScript -> dist/. Este stage nunca
# chega à imagem final, então seu tamanho não afeta a imagem de produção.
FROM node:20-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ---- Stage 2: runtime ---------------------------------------------------
# Imagem enxuta: só dist/, node_modules de produção e package.json.
FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json* ./
RUN npm install --omit=dev && npm cache clean --force

COPY --from=builder /app/dist ./dist

# Usuário não-root (a imagem node:*-alpine já traz o usuário/grupo "node"):
# reduz o blast radius caso o processo Node seja comprometido.
RUN mkdir -p /data && chown -R node:node /data /app
USER node

EXPOSE 3000

# Sinaliza explicitamente que /data deve ser persistido fora da camada
# gravável do container: é onde vivem o WAL e os segmentos .idx/.data
# imutáveis. Sem isso, `docker rm` apagaria todos os logs indexados.
VOLUME ["/data"]

# Usa apenas o runtime Node (sem curl/wget na imagem alpine) para checar
# GET /healthz, que reflete o estado real do WAL e do pool de workers.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.PORT || 3000) + '/healthz').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "dist/server.js"]
