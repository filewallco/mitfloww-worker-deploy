import { connection } from '../queue/connection';

const LIMITS = {
  small: 5,
  medium: 3,
  large: 1,
};

function key(type: keyof typeof LIMITS) {
  return `admission:${type}`;
}

// Atomic LUA script (IMPORTANT)
const acquireScript = `
local current = redis.call("GET", KEYS[1])
if not current then current = 0 else current = tonumber(current) end

if current < tonumber(ARGV[1]) then
  redis.call("INCR", KEYS[1])
  return 1
else
  return 0
end
`;

export async function acquire(type: keyof typeof LIMITS): Promise<boolean> {
  const result = await connection.eval(
    acquireScript,
    1,
    key(type),
    LIMITS[type]
  );

  return result === 1;
}

export async function release(type: keyof typeof LIMITS) {
  await connection.eval(
    `
    local val = redis.call("GET", KEYS[1])
    if val and tonumber(val) > 0 then
      return redis.call("DECR", KEYS[1])
    end
    return 0
    `,
    1,
    key(type)
  );
}