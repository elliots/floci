# The consumers of the messages test (consumer.mjs). The base image is pinned
# by digest and the packages by package-lock.json.
FROM node:24-alpine@sha256:44fce696f38ba8c987c4db89954051000bbd05ffd8d8659ba15ed4a8602e937f
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund && npm cache clean --force
COPY consumer.mjs ./
USER node
CMD ["node", "consumer.mjs"]
