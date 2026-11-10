/**
 * Rendering cost of the live controlboard: opens BOARD_URL (default http://board.localhost:8080)
 * in headless Chromium, lets it settle, then reports main-thread time per wall-clock second over
 * a 20 s window (Chrome DevTools Performance metrics). Lower is better; compare before/after.
 * Usage: bun run perf-check
 */
import { chromium } from "playwright-core";

const url = process.env.BOARD_URL ?? "http://board.localhost:8080/";
const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH ?? "/usr/bin/chromium",
  args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"],
});
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
const cdp = await page.context().newCDPSession(page);
await cdp.send("Performance.enable");
await page.goto(url, { waitUntil: "domcontentloaded" });
await page.waitForTimeout(8000);

type Metrics = Record<string, number>;
const read = async (): Promise<Metrics> => {
  const { metrics } = (await cdp.send("Performance.getMetrics")) as { metrics: { name: string; value: number }[] };
  return Object.fromEntries(metrics.map((m) => [m.name, m.value]));
};
const window = 20;
const a = await read();
await page.waitForTimeout(window * 1000);
const b = await read();
const fps = await page.evaluate(() => {
  const lab = (globalThis as unknown as { __kafkaLab?: { game: { loop: { actualFps: number } } } }).__kafkaLab;
  return lab?.game.loop.actualFps ?? 0;
});
const per = (k: string) => (((b[k] ?? 0) - (a[k] ?? 0)) / window) * 1000;
console.log(`script ${per("ScriptDuration").toFixed(0)} ms/s · task ${per("TaskDuration").toFixed(0)} ms/s · layout ${per("LayoutDuration").toFixed(0)} ms/s · fps ${fps.toFixed(0)}`);
await browser.close();
