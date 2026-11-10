/** Fixed-capacity object pool: reuses released objects instead of allocating per event. */
export class Pool<T> {
  private free: T[] = [];
  readonly live: T[] = [];

  constructor(
    private readonly create: () => T,
    readonly capacity: number,
  ) {}

  /** Returns a recycled (or new) object, or null when `capacity` objects are already live. */
  acquire(): T | null {
    if (this.live.length >= this.capacity) return null;
    const obj = this.free.pop() ?? this.create();
    this.live.push(obj);
    return obj;
  }

  /** Releases every live object for which `dead` returns true (order of the rest is kept). */
  sweep(dead: (obj: T) => boolean): void {
    let w = 0;
    for (let r = 0; r < this.live.length; r++) {
      const obj = this.live[r]!;
      if (dead(obj)) this.free.push(obj);
      else this.live[w++] = obj;
    }
    this.live.length = w;
  }

  get size(): number {
    return this.live.length;
  }

  get pooled(): number {
    return this.free.length;
  }
}
