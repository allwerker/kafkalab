/**
 * Live check against the running cluster: opens http://board.localhost:8080 in headless Chromium,
 * waits for live data, reports console errors and saves smoke/live.png. Usage: bun run live-check
 */
import { join } from "node:path";
import { chromium } from "playwright-core";

const root = join(import.meta.dir, "..");
const url = process.env.BOARD_URL ?? "http://board.localhost:8080/";
const errors: string[] = [];
const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH ?? "/usr/bin/chromium",
  args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"],
});
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.on("console", (m) => m.type() === "error" && errors.push(`console: ${m.text()}`));
page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));
page.on("requestfailed", (r) => errors.push(`requestfailed: ${r.url()} ${r.failure()?.errorText}`));
await page.goto(url, { waitUntil: "domcontentloaded" });
await page.waitForSelector(".live__canvas canvas", { state: "attached", timeout: 20000 }).catch(() => errors.push("live view never rendered"));
await page.waitForTimeout(12000);
await page.screenshot({ path: join(root, "smoke", "live.png") });
await browser.close();
console.log(errors.length ? errors.join("\n") : "no browser errors");
