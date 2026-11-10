import { describe, expect, it } from "vitest";
import { fmtAge, fmtClock, fmtCompact, fmtDuration, fmtEur, fmtInt, fmtMs, fmtPct, humanize, shortId } from "./format";

describe("formatting", () => {
  it("shouldAbbreviateLargeNumbersWhenShowingKpis", () => {
    // GIVEN: values from a quiet lab up to a Black Friday backlog
    // WHEN: they are formatted compactly
    // THEN: small numbers stay readable and big ones get k/M suffixes
    expect(fmtCompact(950)).toBe("950");
    expect(fmtCompact(4.25)).toBe("4.3");
    expect(fmtCompact(7)).toBe("7");
    expect(fmtCompact(1234)).toBe("1.23k");
    expect(fmtCompact(12340)).toBe("12.3k");
    expect(fmtCompact(4_200_000)).toBe("4.20M");
    expect(fmtCompact(42_000_000)).toBe("42.0M");
    expect(fmtInt(12345.6)).toBe("12,346");
  });

  it("shouldFormatMoneyRatesAndLatenciesWhenShowingBusinessKpis", () => {
    // GIVEN: revenue, a success rate and two latencies
    // WHEN: they are formatted
    // THEN: they read like a business dashboard
    expect(fmtEur(172000)).toBe("€172.0k");
    expect(fmtPct(0.953)).toBe("95.3%");
    expect(fmtPct(0.5, 0)).toBe("50%");
    expect(fmtMs(850)).toBe("850 ms");
    expect(fmtMs(2400)).toBe("2.4 s");
  });

  it("shouldFormatDurationsWhenCountingDownExperiments", () => {
    // GIVEN: durations from seconds to hours (and a negative clock skew)
    // WHEN: they are formatted
    // THEN: the most useful units are shown
    expect(fmtDuration(45)).toBe("45s");
    expect(fmtDuration(185)).toBe("3m 05s");
    expect(fmtDuration(3720)).toBe("1h 02m");
    expect(fmtDuration(-3)).toBe("0s");
  });

  it("shouldShowAgesAndClockTimesWhenListingAlerts", () => {
    // GIVEN: an alert that started 75 seconds ago
    const now = Date.parse("2026-01-15T10:01:15Z");
    // WHEN: its age and local time are formatted
    // THEN: the age ticks in minutes and seconds and the clock is HH:MM:SS
    expect(fmtAge("2026-01-15T10:00:00Z", now)).toBe("1m 15s");
    expect(fmtClock("2026-01-15T10:00:00Z")).toMatch(/^\d{2}:00:00$/);
  });

  it("shouldShortenIdsAndHumanizeKeysWhenDisplayingThem", () => {
    // GIVEN: a UUID, a short id and camelCase / dotted keys
    // WHEN: they are prepared for display
    // THEN: ids are cut to 8 chars and keys become words
    expect(shortId("0c7c2c37-31ca-4f80")).toBe("0c7c2c37");
    expect(shortId("abc")).toBe("abc");
    expect(humanize("processingLatencyMs")).toBe("Processing latency ms");
    expect(humanize("orders.created")).toBe("Orders created");
  });
});
