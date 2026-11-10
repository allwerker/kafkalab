import { describe, expect, it } from "vitest";
import { clamp, curvedPath, dist, lerp, pointAt, withLengths } from "./geometry";

describe("geometry", () => {
  it("shouldStartAndEndAtTheStationsWhenALaneIsStraight", () => {
    // GIVEN: two stations 100 units apart and a straight, unshifted lane
    const path = curvedPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0, 0, 10);
    // WHEN: the endpoints and the middle are sampled
    // THEN: the lane runs exactly between them with the right length
    expect(pointAt(path, 0)).toEqual({ x: 0, y: 0 });
    expect(pointAt(path, 1).x).toBeCloseTo(100);
    expect(pointAt(path, 0.5).x).toBeCloseTo(50);
    expect(path.length).toBeCloseTo(100);
  });

  it("shouldShiftPartitionLanesSidewaysWhenAnOffsetIsGiven", () => {
    // GIVEN: a lane going right with a sideways offset of 10 (one partition further out)
    const path = curvedPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0, 10, 4);
    // WHEN: its middle is sampled
    // THEN: it runs parallel, 10 units below
    expect(pointAt(path, 0.5).y).toBeCloseTo(10);
  });

  it("shouldBendTheLaneWhenABendIsGiven", () => {
    // GIVEN: a bent lane between two stations
    const path = curvedPath({ x: 0, y: 0 }, { x: 100, y: 0 }, 0.2, 0);
    // WHEN: its middle is sampled
    // THEN: it bulges sideways and is longer than the straight line
    expect(Math.abs(pointAt(path, 0.5).y)).toBeGreaterThan(5);
    expect(path.length).toBeGreaterThan(100);
  });

  it("shouldClampPositionsWhenAParcelOvershootsTheLane", () => {
    // GIVEN: a short lane
    const path = withLengths([
      { x: 0, y: 0 },
      { x: 10, y: 0 },
    ]);
    // WHEN: positions beyond both ends are requested
    // THEN: they stick to the endpoints
    expect(pointAt(path, -1)).toEqual({ x: 0, y: 0 });
    expect(pointAt(path, 2)).toEqual({ x: 10, y: 0 });
  });

  it("shouldNotDivideByZeroWhenALaneHasDuplicatePoints", () => {
    // GIVEN: a degenerate lane whose two points coincide
    const path = withLengths([
      { x: 5, y: 5 },
      { x: 5, y: 5 },
    ]);
    // WHEN: its middle is sampled
    // THEN: the single point is returned
    expect(pointAt(path, 0.5)).toEqual({ x: 5, y: 5 });
  });

  it("shouldComputeBasicsWhenAskedForDistanceInterpolationAndClamping", () => {
    // GIVEN: a 3-4-5 triangle and simple ranges
    // WHEN: the helpers are used
    // THEN: they return the textbook values
    expect(dist({ x: 0, y: 0 }, { x: 3, y: 4 })).toBe(5);
    expect(lerp(10, 20, 0.25)).toBe(12.5);
    expect(clamp(5, 0, 3)).toBe(3);
    expect(clamp(-1, 0, 3)).toBe(0);
  });
});
