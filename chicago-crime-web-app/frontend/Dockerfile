FROM node:20-alpine AS build

WORKDIR /app

# Copy dependency manifests first for better layer caching.
COPY package*.json ./

RUN npm install --no-audit --no-fund

COPY . .

RUN npm run build

FROM nginx:1.27-alpine

RUN apk add --no-cache curl

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

COPY nginx.conf /etc/nginx/nginx.conf
COPY --from=build /app/dist/ /usr/share/nginx/html/

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fs http://localhost/health || exit 1
