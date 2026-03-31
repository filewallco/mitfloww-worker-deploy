# FileWall Worker

Queue-driven background processing service for FileWall that handles file transformation and preview generation.

Built with **Node.js**, **TypeScript**, **BullMQ**, and **FFmpeg**, this worker processes uploaded files asynchronously using prioritized queues and controlled concurrency.

---

## 🚀 Features

- 🎥 Video processing (compression to 360p, FFmpeg-based)
- 🧵 Queue-based architecture using BullMQ + Redis
- ⚡ Priority handling (small / medium / large files)
- 🔄 Automatic retries with exponential backoff
- 🧠 Concurrency control to optimize CPU usage
- 🧪 Local testing mode (no frontend or cloud storage required)

---

## 🧱 Architecture
