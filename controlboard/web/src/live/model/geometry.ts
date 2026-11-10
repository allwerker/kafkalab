/** Tiny 2D geometry helpers for lane paths (pure, world coordinates). */

export interface Vec {
  x: number;
  y: number;
}

export interface Path {
  points: Vec[];
  /** cumulative length at each point; cum[0] = 0 */
  cum: number[];
  length: number;
}

export function dist(a: Vec, b: Vec): number {
  return Math.hypot(b.x - a.x, b.y - a.y);
}

export function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

export function clamp(v: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, v));
}

/** Quadratic Bezier from a to b bending by `bend` (fraction of the distance) to the left of a→b,
 *  sampled into `segments` pieces, then shifted sideways by `offset` world units. */
export function curvedPath(a: Vec, b: Vec, bend: number, offset: number, segments = 24): Path {
  const mx = (a.x + b.x) / 2;
  const my = (a.y + b.y) / 2;
  const d = dist(a, b);
  const nx = -(b.y - a.y) / d;
  const ny = (b.x - a.x) / d;
  const c = { x: mx + nx * bend * d, y: my + ny * bend * d };
  const raw: Vec[] = [];
  for (let i = 0; i <= segments; i++) {
    const t = i / segments;
    const u = 1 - t;
    raw.push({ x: u * u * a.x + 2 * u * t * c.x + t * t * b.x, y: u * u * a.y + 2 * u * t * c.y + t * t * b.y });
  }
  const points = raw.map((p, i) => {
    const prev = raw[Math.max(0, i - 1)]!;
    const next = raw[Math.min(raw.length - 1, i + 1)]!;
    const len = dist(prev, next);
    return { x: p.x + (-(next.y - prev.y) / len) * offset, y: p.y + ((next.x - prev.x) / len) * offset };
  });
  return withLengths(points);
}

export function withLengths(points: Vec[]): Path {
  const cum = [0];
  for (let i = 1; i < points.length; i++) cum.push(cum[i - 1]! + dist(points[i - 1]!, points[i]!));
  return { points, cum, length: cum[cum.length - 1]! };
}

/** Point at fraction t (0..1, clamped) of the path length. */
export function pointAt(path: Path, t: number): Vec {
  const target = clamp(t, 0, 1) * path.length;
  let i = 1;
  while (i < path.cum.length - 1 && path.cum[i]! < target) i++;
  const a = path.points[i - 1]!;
  const b = path.points[i]!;
  const seg = path.cum[i]! - path.cum[i - 1]!;
  const f = seg > 0 ? (target - path.cum[i - 1]!) / seg : 0;
  return { x: lerp(a.x, b.x, f), y: lerp(a.y, b.y, f) };
}
