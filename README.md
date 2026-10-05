# MitFloww Worker System

High-performance distributed file processing pipeline for images and videos.

---

## 🚀 Architecture Overview

Client → API → Queue (BullMQ) → Workers → Temp FS → Processing → Upload → Cleanup  
                                                      ↓  
                                                   Redis  
                                       (state + locks + limits)

---

## 🧠 Core Principles

### Ephemeral Processing
- Temp files are NOT storage
- Used only during processing
- Always cleaned after lifecycle

### Distributed Safety
- Idempotency locks prevent duplicate execution
- Disk reservation prevents overuse
- CPU limiter prevents overload
- Per-user limits ensure fairness

### Fault Tolerance
- Automatic retries
- Poison job detection
- Dead Letter Queue (DLQ)
- Stuck job recovery

---

## 📦 Job Lifecycle

QUEUED → DOWNLOADING → PROCESSING → UPLOADING → COMPLETED  
                                                ↓  
                                        FAILED / RETRYING

---

## ⚙️ Queues

- small-files
- medium-files
- large-files
- image-files

### Features
- Priority-based scheduling
- Aging to prevent starvation
- Size-based routing
- Tier-based priority

---

## 👷 Workers

| Worker   | Purpose       | Concurrency |
|----------|--------------|------------|
| Fast     | Small files   | High       |
| Standard | Medium files  | Moderate   |
| Heavy    | Large files   | 1          |
| Image    | Images        | High       |

Configured via environment variables:
- FAST_CONCURRENCY
- MEDIUM_CONCURRENCY
- HEAVY_CONCURRENCY
- IMAGE_CONCURRENCY

---

## 🎬 Processing Pipeline

### Image Processing
- Resize (max 1024px)
- Watermark overlay
- Format optimization (webp/png/jpeg)
- Animated support

### Video Processing
- FFmpeg-based pipeline
- Scale to 360p
- Watermark overlay
- H.264 encoding
- Preview clip generation (8s)

---

## ⚡ Resource Management

### CPU Control
- Redis-based global semaphore
- Prevents CPU overload

### Disk Reservation
- Atomic reservation before processing
- Prevents disk exhaustion mid-job

Logic:
- If (free - reserved >= required) → allow
- Else → reject job

### Upload Throttling
- Global upload slot limiter
- Prevents I/O congestion

### Per-User Limits

- Free: 2 concurrent jobs  
- Premium: 4 concurrent jobs  
- VIP: 6 concurrent jobs  

---

## 📁 Temp Storage Strategy

### Structure
/tmp/{jobId}/

Contains:
- Input file
- Intermediate files
- Output file

---

### Cleanup Policy

#### Success
- Deleted immediately

#### Failure
- Retained for 30 minutes
- Then auto-deleted

#### Global Cleanup
- Runs every 60 seconds
- Deletes oldest temp folders
- Triggered when disk is low

---

### Important

Temp is:
- Ephemeral workspace
- NOT persistent storage

---

## 🔁 Retry System

### Levels

1. BullMQ retries  
   - Exponential backoff  

2. Custom requeue  
   - Based on file size  
   - Max total retries: 5  

3. Poison job detection  
   - Stops retry if same error repeats  

---

## 🧑‍💻 Per-User Fairness System

- Each job includes userId
- Redis-based atomic concurrency control
- Ensures no user can monopolize workers

Behavior:
- Bulk uploads are rate-limited per user
- Other users are never blocked

---

## ⚙️ Environment Configuration

Example:
```
MODE=local  

REDIS_HOST=127.0.0.1  
REDIS_PORT=6379  

FFMPEG_PATH=ffmpeg  
FFMPEG_MAX_THREADS=2  
MAX_PARALLEL_UPLOADS=3  

OUTPUT_DIR=./outputs  

FAST_CONCURRENCY=5  
MEDIUM_CONCURRENCY=3  
HEAVY_CONCURRENCY=1  
IMAGE_CONCURRENCY=15  

RATE_LIMIT_MAX=10  
RATE_LIMIT_DURATION=1000  

MIN_FREE_DISK=5368709120  
TARGET_FREE_DISK=10737418240  
MAX_UPLOAD_BYTES=5368709120
MAX_OUTPUT_BYTES=5368709120
MAX_IMAGE_PIXELS=50000000
MAX_PDF_BYTES=104857600
MAX_PDF_PAGES=100
MAX_PDF_PAGE_DIMENSION=14400
PDF_PROCESSING_TIMEOUT_MS=60000

PORT=4000  
WS_PORT=4001  
ADMIN_TOKEN=
WS_TOKEN=
```

---


## 🧪 Local Testing

### Start Redis (Docker)

```bash
docker run -d -p 6379:6379 --name mitfloww-redis redis  
docker stop mitfloww-redis  
docker start mitfloww-redis  
```

### Worker Container Hardening

Run production worker containers with `--read-only`, `--cap-drop=ALL`, `--security-opt=no-new-privileges`, `--pids-limit`, explicit `--memory` / `--cpus`, writable mounts only for temp/output paths, and admin/WS ports on a private network.

---

### Run Worker Server

pnpm ts-node src/index.ts  

---

### Notes

- Use local files with:
  file://absolute/path/to/file

- Default test values:
  - userId: local-user
  - batchId: local-batch

---

## ⚠️ Key Guarantees

- No duplicate job execution  
- No disk overcommit  
- No CPU overload  
- Fair usage across users  
- Automatic recovery from failures  

---

## 🚀 Future Improvements

- Batch progress tracking  
- Adaptive scheduling (dynamic priority)  
- Smarter retry classification  
- Distributed tracing  
- AI-based moderation (optional)  

---

## 🐳 Production Docker Deployment (Oracle Cloud ARM64 / Ubuntu)

The worker is fully containerized using a multi-stage, non-root Docker architecture compatible with both `linux/arm64` (Oracle Cloud Ampere A1) and `linux/amd64`.

### 1. Install Docker & Docker Compose
On Ubuntu / Debian (x86_64 or aarch64):
```bash
# Update and install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
newgrp docker

# Verify Docker installation
docker --version
docker compose version
```

### 2. Clone Worker Repository
```bash
git clone <repository-url> mitfloww
cd mitfloww/worker
```

### 3. Create .env
```bash
cp .env.example .env
```

### 4. Configure Secrets
Edit `.env` and provide production secrets:
```bash
nano .env
```
Ensure the following variables are configured:
- `REDIS_URL`: Connection string to your production Redis instance (e.g., `redis://:password@redis.internal:6379`).
- `WORKER_API_TOKEN`: Shared secret for internal Next.js API requests.
- `PROCESSING_CALLBACK_TOKEN`: Secret used when reporting job results back to Next.js API.
- `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET_NAME`: Cloudflare R2 credentials.
- `R2_PUBLIC_BASE_URL`: Public CDN / R2 domain for media streaming.

### 5. Build Image
Build the container image locally:
```bash
docker compose build
```

To build specifically for ARM64 using Docker Buildx:
```bash
docker buildx build --platform linux/arm64 -t mitfloww-worker:latest .
```

### 6. Start Worker
Start the worker in detached background mode:
```bash
docker compose up -d
```

Verify the container status:
```bash
docker compose ps
```

### 7. Check Logs
Monitor real-time worker logs:
```bash
docker compose logs -f worker
```

Verify internal versions and health inside the container:
```bash
docker compose exec worker node --version
docker compose exec worker ffmpeg -version
docker compose exec worker node -e "const http = require('http'); http.get('http://127.0.0.1:4000/health', (res) => { res.pipe(process.stdout); });"
```

### 8. Update & Redeploy
When deploying new code changes:
```bash
git pull origin main
docker compose up -d --build
docker compose logs -f worker
```

### 9. Restart Worker
Restart worker cleanly (triggers `SIGTERM` graceful shutdown):
```bash
docker compose restart worker
```
To stop the service:
```bash
docker compose down
```

### 10. Troubleshooting

- **Container fails health check:**
  Check container logs:
  ```bash
  docker compose logs --tail=100 worker
  ```
  Ensure Redis is reachable from inside the container.

- **Redis connection errors:**
  Verify that `REDIS_URL` in `.env` is accessible and any Redis firewall / VPC security lists allow incoming connections on port 6379.

- **FFmpeg encoding errors:**
  Inspect job logs in Redis or container output. Verify that `FFMPEG_MAX_THREADS` is set appropriately for your VM size (e.g. 2 for 4-vCPU VM).

- **Temporary disk fill-up:**
  Worker temp directory is mounted on the `worker-tmp` volume. The worker runs automatic cleanup every 60s and emergency disk cleanup when space drops below `TARGET_FREE_DISK`. To inspect volume size:
  ```bash
  docker system df -v
  ```

---

## 🧭 Summary

This system is designed for:

- High reliability  
- Fair resource distribution  
- Efficient media processing  
- Horizontal scalability  

Temp storage is controlled, failures are handled, and no single user can degrade system performance.
