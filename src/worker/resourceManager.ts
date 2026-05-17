import os from 'os';
import { config } from '../config';
import { JOB_STATUS, REDIS_KEYS } from '../constants';
import { connection } from '../queue/connection';
import { imageQueue, largeQueue, mediumQueue, smallQueue } from '../queue/queues';
import { getDiskCapacity as getFsDiskCapacity, getFreeDiskSpace as getFsFreeDiskSpace } from '../utils/disk';
import { logger } from '../utils/logger';
import { FileJob } from '../types';

const RESERVE_DISK_LUA = `
local reservedTotal = tonumber(redis.call("GET", KEYS[1]) or "0")
local existingReservation = redis.call("GET", KEYS[2])
if existingReservation then
  return 1
end

local physicalFree = tonumber(ARGV[1])
local minFreeBytes = tonumber(ARGV[2])
local requiredBytes = tonumber(ARGV[3])

if (physicalFree - reservedTotal - minFreeBytes) >= requiredBytes then
  redis.call("SET", KEYS[2], requiredBytes)
  redis.call("INCRBY", KEYS[1], requiredBytes)
  return 1
end

return 0
`;

const RELEASE_DISK_LUA = `
local reservation = tonumber(redis.call("GET", KEYS[2]) or "0")
if reservation <= 0 then
  redis.call("DEL", KEYS[2])
  return 0
end

local reservedTotal = tonumber(redis.call("GET", KEYS[1]) or "0")
local nextTotal = reservedTotal - reservation
if nextTotal < 0 then
  nextTotal = 0
end

if nextTotal == 0 then
  redis.call("DEL", KEYS[1])
else
  redis.call("SET", KEYS[1], nextTotal)
end

redis.call("DEL", KEYS[2])
return reservation
`;

const ACQUIRE_SET_SLOT_LUA = `
if redis.call("SISMEMBER", KEYS[1], ARGV[1]) == 1 then
  return 1
end

local limit = tonumber(ARGV[2])
if redis.call("SCARD", KEYS[1]) < limit then
  redis.call("SADD", KEYS[1], ARGV[1])
  return 1
end

return 0
`;

export function getDiskCapacity(): number {
  return getFsDiskCapacity();
}

export function getFreeDiskBytes(): number {
  return getFsFreeDiskSpace();
}

export function getFreeDiskSpace(): number {
  return getFreeDiskBytes();
}

export function estimateRequiredDisk(job: Pick<FileJob, 'size'>, sourceSizeBytes?: number): number {
  const baseSize = Number.isFinite(sourceSizeBytes) && sourceSizeBytes && sourceSizeBytes > 0
    ? sourceSizeBytes
    : job.size;
  return Math.ceil(
    Math.max(
      baseSize * config.disk.reservationMultiplier,
      config.disk.reservationMinBytes,
    ),
  );
}

export function canEverFitJob(requiredDisk: number): boolean {
  const capacity = getDiskCapacity();
  return capacity - config.disk.minFreeBytes >= requiredDisk;
}

export async function tryReserveDisk(jobId: string, bytes: number): Promise<boolean> {
  const free = getFreeDiskBytes();
  const result = await connection.eval(
    RESERVE_DISK_LUA,
    2,
    REDIS_KEYS.RESOURCE_DISK_RESERVED_TOTAL,
    REDIS_KEYS.RESOURCE_DISK_JOB(jobId),
    Math.floor(free),
    Math.floor(config.disk.minFreeBytes),
    Math.floor(bytes),
  );
  return Number(result) === 1;
}

export async function releaseDisk(jobId: string): Promise<void> {
  await connection.eval(
    RELEASE_DISK_LUA,
    2,
    REDIS_KEYS.RESOURCE_DISK_RESERVED_TOTAL,
    REDIS_KEYS.RESOURCE_DISK_JOB(jobId),
  );
}

export async function refreshReservations(jobId: string): Promise<void> {
  await connection.hset(REDIS_KEYS.JOB(jobId), {
    reservationHeartbeatAt: Date.now(),
  });
}

export async function getReservedDiskTotal(): Promise<number> {
  const raw = await connection.get(REDIS_KEYS.RESOURCE_DISK_RESERVED_TOTAL);
  return raw ? Number(raw) : 0;
}

export async function getJobReservation(jobId: string): Promise<number> {
  const raw = await connection.get(REDIS_KEYS.RESOURCE_DISK_JOB(jobId));
  return raw ? Number(raw) : 0;
}

function getGlobalCpuLimit(): number {
  if (config.resource.globalCpuLimit > 0) {
    return config.resource.globalCpuLimit;
  }

  try {
    const fs = require('fs');
    const data = fs.readFileSync('/sys/fs/cgroup/cpu.max', 'utf8');
    const [quota, period] = String(data).trim().split(' ');
    if (quota !== 'max') {
      const cores = Math.floor(Number(quota) / Number(period));
      if (Number.isFinite(cores) && cores > 0) {
        return cores;
      }
    }
  } catch {
    // noop
  }

  return Math.max(1, os.cpus().length);
}

const GLOBAL_CPU_LIMIT = getGlobalCpuLimit();
const SAFE_JOB_ID = /^[A-Za-z0-9_-]{1,128}$/;
const TERMINAL_JOB_STATUSES = new Set<string>([
  JOB_STATUS.COMPLETED,
  JOB_STATUS.FAILED,
  JOB_STATUS.CANCELLED,
]);

export type CpuLane = 'image' | 'small' | 'medium' | 'heavy';

function getCpuLaneKey(lane: CpuLane): string {
  if (lane === 'image') return REDIS_KEYS.RESOURCE_CPU_IMAGE_HOLDERS;
  if (lane === 'small') return REDIS_KEYS.RESOURCE_CPU_SMALL_HOLDERS;
  if (lane === 'medium') return REDIS_KEYS.RESOURCE_CPU_MEDIUM_HOLDERS;
  return REDIS_KEYS.RESOURCE_CPU_HEAVY_HOLDERS;
}

function getCpuLaneLimit(lane: CpuLane): number {
  if (lane === 'image') return Math.max(1, config.resource.imageCpuLimit);
  if (lane === 'small') return Math.max(1, config.resource.smallCpuLimit);
  if (lane === 'medium') return Math.max(1, config.resource.mediumCpuLimit);
  return Math.max(1, config.resource.heavyCpuLimit);
}

export async function tryAcquireCpuLane(
  jobId: string,
  lane: CpuLane,
): Promise<boolean> {
  const ok = await connection.eval(
    ACQUIRE_SET_SLOT_LUA,
    1,
    getCpuLaneKey(lane),
    jobId,
    getCpuLaneLimit(lane),
  );

  return Number(ok) === 1;
}

export async function releaseCpuLane(
  jobId: string,
  lane: CpuLane,
): Promise<void> {
  await connection.srem(getCpuLaneKey(lane), jobId);
}

/**
 * Legacy compatibility.
 * Prefer tryAcquireCpuLane/releaseCpuLane in new code.
 */
export async function tryAcquireCpu(jobId: string): Promise<boolean> {
  const ok = await connection.eval(
    ACQUIRE_SET_SLOT_LUA,
    1,
    REDIS_KEYS.RESOURCE_CPU_HOLDERS,
    jobId,
    GLOBAL_CPU_LIMIT,
  );
  return Number(ok) === 1;
}

export async function releaseCpu(jobId: string): Promise<void> {
  await connection.srem(REDIS_KEYS.RESOURCE_CPU_HOLDERS, jobId);
}

export async function tryAcquireUserSlot(userId: string, jobId: string, limit: number): Promise<boolean> {
  const ok = await connection.eval(
    ACQUIRE_SET_SLOT_LUA,
    1,
    REDIS_KEYS.RESOURCE_USER_HOLDERS(userId),
    jobId,
    limit,
  );
  return Number(ok) === 1;
}

export async function releaseUserSlot(userId: string, jobId: string): Promise<void> {
  await connection.srem(REDIS_KEYS.RESOURCE_USER_HOLDERS(userId), jobId);
}

export async function tryAcquireUploadSlot(holderId: string): Promise<boolean> {
  const ok = await connection.eval(
    ACQUIRE_SET_SLOT_LUA,
    1,
    REDIS_KEYS.RESOURCE_UPLOAD_HOLDERS,
    holderId,
    config.resource.maxParallelUploads,
  );
  return Number(ok) === 1;
}

export async function releaseUploadSlot(holderId: string): Promise<void> {
  await connection.srem(REDIS_KEYS.RESOURCE_UPLOAD_HOLDERS, holderId);
}

async function scanKeys(pattern: string): Promise<string[]> {
  let cursor = '0';
  const keys: string[] = [];

  do {
    const [nextCursor, batch] = await connection.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = nextCursor;
    keys.push(...batch);
  } while (cursor !== '0');

  return keys;
}

async function getBullState(jobId: string): Promise<string | null> {
  const job =
    (await imageQueue.getJob(jobId)) ||
    (await smallQueue.getJob(jobId)) ||
    (await mediumQueue.getJob(jobId)) ||
    (await largeQueue.getJob(jobId));

  if (!job) return null;
  return await job.getState();
}

function getHolderJobId(holderId: string): string | null {
  if (SAFE_JOB_ID.test(holderId)) {
    return holderId;
  }

  const uploadMatch = holderId.match(/^([A-Za-z0-9_-]{1,128}):upload$/);
  return uploadMatch ? uploadMatch[1] : null;
}

async function reconcileHolderSet(resourceKey: string, resourceType: 'cpu' | 'upload' | 'user') {
  const holders = await connection.smembers(resourceKey);
  if (holders.length === 0) return;

  const now = Date.now();
  const staleThresholdMs = Number(process.env.STUCK_JOB_THRESHOLD_MS || 30 * 60 * 1000);

  for (const holderId of holders) {
    const jobId = getHolderJobId(holderId);

    if (!jobId) {
      await connection.srem(resourceKey, holderId);
      logger.warn('Removed stale resource holder', {
        resourceType,
        resourceKey,
        holderId,
        reason: 'unparseable_holder_id',
      });
      continue;
    }

    const [bullState, lockExists, meta] = await Promise.all([
      getBullState(jobId),
      connection.exists(REDIS_KEYS.LOCK(jobId)),
      connection.hgetall(REDIS_KEYS.JOB(jobId)),
    ]);

    const status = meta.status || '';
    const heartbeatAt = Number(meta.heartbeatAt || meta.updatedAt || 0);
    const heartbeatFresh = heartbeatAt > 0 && now - heartbeatAt < staleThresholdMs;
    const terminalOrMissingStatus = !status || TERMINAL_JOB_STATUSES.has(status);
    const isStale =
      bullState !== 'active' &&
      (terminalOrMissingStatus || !lockExists || !heartbeatFresh);

    if (!isStale) {
      continue;
    }

    const reason = terminalOrMissingStatus
      ? 'terminal_or_missing_status'
      : !lockExists
        ? 'missing_lock'
        : 'stale_heartbeat';

    await connection.srem(resourceKey, holderId);
    logger.warn('Removed stale resource holder', {
      resourceType,
      resourceKey,
      holderId,
      jobId,
      reason,
      bullState,
      status: status || null,
      heartbeatAt: heartbeatAt || null,
    });
  }
}

export async function reconcileResourceHolders(): Promise<void> {
  const userHolderKeys = await scanKeys('resource:user:*:holders');

  await reconcileHolderSet(REDIS_KEYS.RESOURCE_CPU_HOLDERS, 'cpu'); // legacy

  await reconcileHolderSet(REDIS_KEYS.RESOURCE_CPU_IMAGE_HOLDERS, 'cpu');
  await reconcileHolderSet(REDIS_KEYS.RESOURCE_CPU_SMALL_HOLDERS, 'cpu');
  await reconcileHolderSet(REDIS_KEYS.RESOURCE_CPU_MEDIUM_HOLDERS, 'cpu');
  await reconcileHolderSet(REDIS_KEYS.RESOURCE_CPU_HEAVY_HOLDERS, 'cpu');

  await reconcileHolderSet(REDIS_KEYS.RESOURCE_UPLOAD_HOLDERS, 'upload');

  for (const userHolderKey of userHolderKeys) {
    await reconcileHolderSet(userHolderKey, 'user');
  }
}
