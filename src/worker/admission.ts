import { connection } from '../queue/connection';

/**
 * Limits for concurrent processing per job size.
 */
const LIMITS = {
  small: 5,
  medium: 3,
  large: 1,
};

/**
 * Redis key for tracking active jobs of a given type.
 * @param type - 'small' | 'medium' | 'large'
 * @returns Redis key string
 */
function key(type: keyof typeof LIMITS) {
  return `admission:${type}`;
}

// LUA script for atomic slot acquisition in Redis
// NOTE: Removed TTL to avoid concurrency leakage during long jobs
const acquireScript = `
local current = redis.call("GET", KEYS[1])
if not current then
  redis.call("SET", KEYS[1], 1)
  return 1
end

current = tonumber(current)

if current < tonumber(ARGV[1]) then
  redis.call("INCR", KEYS[1])
  return 1
else
  return 0
end
`;

/**
 * Attempt to acquire a slot for processing a job of given type.
 * @param type - Job type: 'small', 'medium', 'large'
 * @returns true if slot acquired, false otherwise
 */
export async function acquire(type: keyof typeof LIMITS): Promise<boolean> {
  const result = await connection.eval(acquireScript, 1, key(type), LIMITS[type]);
  return result === 1;
}

/**
 * Release a previously acquired slot.
 * @param type - Job type: 'small', 'medium', 'large'
 */
export async function release(type: keyof typeof LIMITS) {
  await connection.eval(
    `
    local val = redis.call("GET", KEYS[1])
    if val and tonumber(val) > 0 then
      local val = redis.call("DECR", KEYS[1])
      if val <= 0 then
        redis.call("DEL", KEYS[1])
      end
      return val
    end
    return 0
    `,
    1,
    key(type)
  );
}