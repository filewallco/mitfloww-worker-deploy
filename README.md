# File Processing Worker (MitFloww) – README

## Overview

This service is a **background worker system** designed to process files (primarily videos in Phase 1) asynchronously. It handles:

* File ingestion (download)
* Processing (e.g., video compression + watermarking)
* Upload (local or cloud/R2)
* Status tracking (Redis-based)
* Queueing, prioritization, retries, and concurrency control

This is designed to integrate with a frontend (e.g., Next.js) where users upload files and later check job status.

---

# Architecture

## High-Level Flow

```
Client (Next.js)
      ↓
API (enqueue job)
      ↓
BullMQ Queue (Redis)
      ↓
Workers (Fast / Standard / Heavy)
      ↓
Handler
  ├── Download
  ├── Process (FFmpeg)
  ├── Upload (R2 / Local)
  └── Update Status (Redis)
      ↓
Client polls job status
```

---

## Worker Types

| Worker         | Handles     | Concurrency |
| -------------- | ----------- | ----------- |
| fastWorker     | small files | 2           |
| standardWorker | all jobs    | 3           |
| heavyWorker    | large files | 1           |

---

## Scheduling Strategy

* **Priority Queue (BullMQ)**
* Based on:

  * User tier (VIP > Premium > Free)
  * File size (Small > Medium > Large)
* **Retries with exponential backoff**
* **Admission control (Redis-based concurrency limits)**

---

# Core Components

## 1. Queue (BullMQ)

* Manages job lifecycle
* Supports:

  * Priority
  * Retries
  * Delayed execution

### Why BullMQ?

* Reliable job processing
* Redis-backed durability
* Built-in retry + backoff
* Horizontal scalability

---

## 2. Redis (ioredis)

Used for:

* Queue backend
* Job metadata storage
* Admission control (distributed concurrency limits)

### Why Redis?

* Extremely fast
* Atomic operations (Lua scripts)
* Perfect for ephemeral job state

---

## 3. FFmpeg

Used for:

* Video processing
* Compression
* Watermarking

### Why FFmpeg?

* Industry standard
* Highly optimized
* Supports all major formats

---

## 4. Worker Handler

Core execution pipeline:

```
acquire slot → download → process → upload → update status → release slot
```

Responsibilities:

* Admission control
* Temp file management
* Error handling
* Status updates

---

## 5. Admission Control

Located in:

```
src/worker/admission.ts
```

Purpose:

* Prevent system overload
* Limit concurrent jobs per size category

Example:

```
small  → 5 concurrent
medium → 3 concurrent
large  → 1 concurrent
```

Uses Redis + Lua for atomicity.

---

## 6. Job Status Tracking

Stored in Redis:

```
job:{fileId}
```

Example fields:

```
status: queued | processing | completed | failed | retrying
stage: downloading | processing | uploading | done
progress: number
output: file URL/path
duration: ms
error: message
```

---

## Project Structure

```
src/
├── processors/
│   └── video.ts        # FFmpeg processing
│
├── queue/
│   ├── connection.ts   # Redis connection
│   ├── enqueue.ts      # Add jobs
│   └── queues.ts       # BullMQ queue
│
├── worker/
│   ├── admission.ts    # Concurrency control
│   ├── handler.ts      # Core job execution
│   ├── fastWorker.ts
│   ├── standardWorker.ts
│   └── heavyWorker.ts
│
├── utils/
│   └── r2.ts           # Download/upload logic
│
├── server/
│   └── jobStatus.ts    # Fetch job status
│
├── localTest.ts        # Local testing runner
├── config.ts           # Centralized configuration
├── types.ts            # Structure of file processing
└── index.ts            # Entry point
```

---

# Setup & Requirements

## 1. System Requirements

* Node.js (>= 18)
* Redis server
* FFmpeg installed

### Install FFmpeg

**Ubuntu**

```
sudo apt install ffmpeg
```

**Mac**

```
brew install ffmpeg
```

---

## 2. Install Dependencies

```
pnpm install
```

---

## 3. Environment Variables

Create `.env.local`:

```
MODE=local

REDIS_HOST=127.0.0.1
REDIS_PORT=6379

FFMPEG_PATH=ffmpeg

OUTPUT_DIR=./outputs
```

---

## 4. Start Redis

```
redis-server
```

---

## 5. Run Worker

```
pnpm run dev
```

---

## 6. Local Testing

Place files inside:

```
/test-files
```

Then run:

```
pnpm run dev
```

---

# How It Works (Step-by-Step)

### 1. Enqueue Job

```
enqueueFile(job)
```

* Stores initial status in Redis
* Adds job to BullMQ

---

### 2. Worker Picks Job

* Based on type (small/medium/large)
* Runs `handleJob`

---

### 3. Admission Control

```
acquire(type)
```

* Prevents overload
* If full → job retries

---

### 4. Processing Pipeline

```
download → processVideo → upload
```

---

### 5. Status Updates

Stored continuously in Redis:

* Real-time progress
* Stage updates
* Errors

---

### 6. Completion

* Output stored (local path or URL)
* Temp files deleted

---

# API Integration (Frontend)

Use:

```
getJobStatus(jobId)
```

Returns:

```
{
  queueState,
  progress,
  meta: {
    status,
    stage,
    output,
    duration,
    error
  }
}
```

Frontend should:

* Poll every 2–5 seconds
* Display progress/status
* Show download link on completion

---

# Phase 1 Scope

### Supported

* Video processing
* Watermarking (FFmpeg)
* Local + mock cloud upload
* Queue + retries
* Status tracking

### Not Included Yet

* Real R2/S3 upload
* DB persistence (Redis only)
* Multi-region scaling
* Advanced scheduling

---

## To run the server

***Start Docker***

```bash
docker run -d -p 6379:6379 --name mitfloww-redis redis
docker stop mitfloww-redis
docker start mitfloww-redis
```

***Run node server***

```bash
pnpm ts-node src/index.ts
```

---

## Usefull docker commands

***Verify docker running***

```bash
docker ps
```

***To remove container***

```bash
docker rm <container_id>
```

---