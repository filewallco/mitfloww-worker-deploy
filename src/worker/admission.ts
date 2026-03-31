export const LIMITS = {
  SMALL: 10,   // no limit effectively
  MEDIUM: 7,
  LARGE: 6,
};

let running = {
  small: 0,
  medium: 0,
  large: 0,
};

export function canRun(type: keyof typeof running) {
  return running[type] < LIMITS[type.toUpperCase() as keyof typeof LIMITS];
}

export function start(type: keyof typeof running) {
  running[type]++;
}

export function done(type: keyof typeof running) {
  running[type]--;
}