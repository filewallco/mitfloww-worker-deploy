import { getPriority } from '../queue/priority';
import { smallQueue, mediumQueue, largeQueue, imageQueue } from '../queue/queues';

/**
 * Rebalances priorities of jobs currently waiting in a given queue.
 *
 * Why:
 * - BullMQ computes priority only at enqueue time (static)
 * - This function applies "aging" by recomputing priority over time
 * - Prevents starvation of lower-priority jobs
 *
 * Strategy:
 * - Fetch a small batch of waiting jobs (bounded for performance)
 * - Recalculate priority using latest wait time
 * - Update job priority in-place
 *
 * @param queue - BullMQ queue instance
 */
async function rebalanceQueue(queue: any) {
  // Fetch a limited batch to avoid heavy Redis load
  const jobs = await queue.getWaiting(0, 50);

  for (const job of jobs) {
    try {
      /**
       * Recompute priority using current waiting time.
       * job.data contains original FileJob payload.
       */
      const newPriority = await getPriority({
        ...job.data,
        fileId: job.id, // ensure correct jobId for Redis lookup
      });

      /**
       * Update priority only if job is still in waiting state.
       * changePriority is a BullMQ operation that reorders the job in queue.
       */
      await job.changePriority({ priority: newPriority });

    } catch (err) {
      /**
       * Non-blocking failure:
       * - Do not crash scheduler
       * - Log for observability
       */
      // Import logger lazily to avoid circular deps in some environments
      const { logger } = await import('../utils/logger');
      logger.error('Priority update failed', { jobId: job.id, error: err });
    }
  }
}

/**
 * Starts the global priority scheduler.
 *
 * Behavior:
 * - Runs periodically (every 60 seconds)
 * - Rebalances all queues in parallel
 *
 * Design considerations:
 * - Interval-based approach (simple, predictable)
 * - Batch-limited scans to avoid Redis overload
 * - Eventually consistent fairness (not real-time)
 */
export function startPriorityScheduler() {
  setInterval(async () => {
    await Promise.all([
      rebalanceQueue(smallQueue),
      rebalanceQueue(mediumQueue),
      rebalanceQueue(largeQueue),
      rebalanceQueue(imageQueue),
    ]);
  }, 60_000);
}