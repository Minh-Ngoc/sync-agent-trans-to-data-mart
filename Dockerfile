# syntax=docker/dockerfile:1

# ---- deps: cai dependency rieng 1 layer de tan dung cache ----
FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json ./
COPY package-lock.json* ./
# Neu co package-lock.json thi dung "npm ci" (nhanh + reproducible),
# khong thi fallback ve "npm install".
RUN if [ -f package-lock.json ]; then \
      npm ci --omit=dev --no-audit --no-fund; \
    else \
      npm install --omit=dev --no-audit --no-fund; \
    fi

# ---- runtime: image cuoi cung, gon nhe ----
FROM node:20-alpine AS runtime

# tini lam PID 1 de forward SIGINT/SIGTERM dung cach vao Node process
# (script co logic "dung an toan sau khi batch dang xu ly commit xong"
# trong setupSignalHandlers() -- can tin hieu toi dung process, khong bi
# shell wrapper nuot mat).
RUN apk add --no-cache tini

WORKDIR /app
ENV NODE_ENV=production

COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY sync-mongo-to-pg.js ./

# Chay bang user non-root co san trong image node:alpine (uid 1000)
USER node

# Gioi han heap cua tien trinh MASTER (moi worker thread da tu gioi han rieng
# qua WORKER_MAX_OLD_GEN_MB / resourceLimits trong chinh script). Day la
# tuyen phong thu thu 2, doc lap voi --memory cua container (docker-compose.yml).
ENV NODE_OPTIONS="--max-old-space-size=512"

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "sync-mongo-to-pg.js"]