import { describe, expect, it } from "vitest";
import { Pool } from "./pool";

describe("object pool", () => {
  it("shouldReuseReleasedObjectsWhenNewParcelsAreNeeded", () => {
    // GIVEN: a pool with one parcel that already finished its trip
    let created = 0;
    const pool = new Pool(() => ({ n: ++created, dead: false }), 10);
    const first = pool.acquire()!;
    first.dead = true;
    pool.sweep((o) => o.dead);
    expect(pool.pooled).toBe(1);
    // WHEN: the next parcel is needed
    const second = pool.acquire()!;
    // THEN: the same object is recycled instead of allocating a new one
    expect(second).toBe(first);
    expect(created).toBe(1);
    expect(pool.size).toBe(1);
  });

  it("shouldRefuseNewObjectsWhenTheCapacityIsReached", () => {
    // GIVEN: a pool for at most two objects, both in use
    const pool = new Pool(() => ({}), 2);
    pool.acquire();
    pool.acquire();
    // WHEN: a third is requested
    // THEN: none is handed out
    expect(pool.acquire()).toBeNull();
    expect(pool.capacity).toBe(2);
  });

  it("shouldKeepTheOrderOfSurvivorsWhenSweeping", () => {
    // GIVEN: three live objects, the middle one finished
    const pool = new Pool(() => ({ id: 0, dead: false }), 5);
    [1, 2, 3].forEach((id) => Object.assign(pool.acquire()!, { id, dead: id === 2 }));
    // WHEN: the pool is swept
    pool.sweep((o) => o.dead);
    // THEN: the survivors keep their draw order
    expect(pool.live.map((o) => o.id)).toEqual([1, 3]);
  });
});
