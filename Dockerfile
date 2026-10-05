# ==============================================================================
# Stage 1: Build stage (compile TypeScript & prepare production dependencies)
# ==============================================================================
FROM node:22-bookworm-slim AS builder

WORKDIR /app

# Enable Corepack and activate exact pnpm version defined in package.json
RUN corepack enable && corepack prepare pnpm@10.33.0 --activate

# Copy dependency manifests
COPY package.json pnpm-lock.yaml tsconfig.json ./

# Install all dependencies (including devDependencies needed for compilation)
RUN pnpm install --frozen-lockfile

# Copy TypeScript source code and assets
COPY src/ ./src/
COPY assets/ ./assets/

# Compile TypeScript to dist/
RUN pnpm run build

# Prune devDependencies to keep only production dependencies
RUN pnpm prune --prod

# ==============================================================================
# Stage 2: Production runtime stage
# ==============================================================================
FROM node:22-bookworm-slim AS runner

LABEL maintainer="MitFloww Team"
LABEL description="MitFloww Production Distributed Worker Service"

# Set default production environment variables
ENV NODE_ENV=production
ENV PORT=4000
ENV WS_PORT=4002
ENV TEMP_DIR=/app/tmp
ENV OUTPUT_DIR=/app/outputs
ENV FFMPEG_PATH=ffmpeg
ENV FFPROBE_PATH=ffprobe

# Install runtime OS packages:
# - ffmpeg: required for video/audio processing (includes ffprobe)
# - ca-certificates: required for secure HTTPS/TLS requests (Cloudflare R2, AWS S3)
# - tini: lightweight init system to handle signal forwarding (SIGTERM) and zombie process reaping
# - fonts-dejavu-core & fontconfig: font glyphs for SVG/text watermark rendering via sharp/librsvg
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    tini \
    fonts-dejavu-core \
    fontconfig \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create necessary runtime directories and ensure ownership by non-root 'node' user
RUN mkdir -p /app/tmp /app/outputs /app/logs \
  && chown -R node:node /app

# Copy production node_modules and package manifest from builder stage
COPY --from=builder --chown=node:node /app/node_modules ./node_modules
COPY --from=builder --chown=node:node /app/package.json ./package.json

# Copy compiled JavaScript from builder stage
COPY --from=builder --chown=node:node /app/dist ./dist

# Copy static assets (watermark images and audio)
COPY --from=builder --chown=node:node /app/assets ./assets

# Switch to non-root user for security
USER node

# Health check targeting the internal admin API
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "const http = require('http'); const req = http.request({ host: '127.0.0.1', port: process.env.PORT || 4000, path: '/health', timeout: 3000 }, (res) => { process.exit(res.statusCode === 200 ? 0 : 1); }); req.on('error', () => process.exit(1)); req.end();"

# Use tini to guarantee proper signal handling (SIGTERM) and child-process cleanup
ENTRYPOINT ["/usr/bin/tini", "--"]

# Launch the worker process
CMD ["node", "dist/index.js"]
