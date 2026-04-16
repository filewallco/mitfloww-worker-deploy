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

PORT=4000  
WS_PORT=4001  
```

---


## 🧪 Local Testing

### Start Redis (Docker)

```bash
docker run -d -p 6379:6379 --name mitfloww-redis redis  
docker stop mitfloww-redis  
docker start mitfloww-redis  
```
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

## 🧭 Summary

This system is designed for:

- High reliability  
- Fair resource distribution  
- Efficient media processing  
- Horizontal scalability  

Temp storage is controlled, failures are handled, and no single user can degrade system performance.