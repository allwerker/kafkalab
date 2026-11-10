/**
 * Browser smoke test: build, start the mock backend + `vite preview`, open the board in headless
 * Chromium, wait for the live-view canvas and the alert panel, save smoke/screenshot.png and fail
 * on any console error or page error.
 *
 * Chromium lookup (first hit wins): $CHROME_PATH, Playwright's own cache
 * ($PLAYWRIGHT_BROWSERS_PATH or ~/.cache/ms-playwright), /usr/bin/chromium, /usr/bin/google-chrome.
 * Flags: --no-build (reuse dist/), SMOKE_EXTRA=1 (also capture tab screenshots).
 */
import { existsSync, mkdirSync, readdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { chromium } from "playwright-core";

const root = join(import.meta.dir, "..");
const MOCK_PORT = 18090;
const PREVIEW_PORT = 14173;

function findChromium(): string {
  if (process.env.CHROME_PATH && existsSync(process.env.CHROME_PATH)) return process.env.CHROME_PATH;
  const cache = process.env.PLAYWRIGHT_BROWSERS_PATH ?? join(homedir(), ".cache", "ms-playwright");
  if (existsSync(cache)) {
    for (const dir of readdirSync(cache).filter((d) => d.startsWith("chromium-")).sort().reverse()) {
      const exe = join(cache, dir, "chrome-linux64", "chrome");
      if (existsSync(exe)) return exe;
    }
  }
  for (const p of ["/usr/bin/chromium", "/usr/bin/chromium-browser", "/usr/bin/google-chrome"]) if (existsSync(p)) return p;
  throw new Error("No Chromium found. Set CHROME_PATH or install one into PLAYWRIGHT_BROWSERS_PATH.");
}

async function waitFor(url: string, timeoutMs = 20000): Promise<void> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    try {
      const r = await fetch(url);
      if (r.ok || r.status === 404) return;
    } catch {
      /* not yet */
    }
    await Bun.sleep(200);
  }
  throw new Error(`timeout waiting for ${url}`);
}

const procs: Bun.Subprocess[] = [];
const cleanup = () => procs.forEach((p) => p.kill());
process.on("exit", cleanup);

let failed = false;
const errors: string[] = [];
let shot: (() => Promise<unknown>) | null = null;
try {
  if (!process.argv.includes("--no-build")) {
    const b = Bun.spawnSync(["bun", "run", "build"], { cwd: root, stdout: "inherit", stderr: "inherit" });
    if (b.exitCode !== 0) throw new Error("build failed");
  }
  procs.push(Bun.spawn(["bun", "run", "mock/server.ts"], { cwd: root, env: { ...process.env, MOCK_PORT: String(MOCK_PORT) }, stdout: "ignore", stderr: "inherit" }));
  procs.push(
    Bun.spawn(["bunx", "vite", "preview", "--host", "127.0.0.1", "--port", String(PREVIEW_PORT), "--strictPort"], {
      cwd: root,
      env: { ...process.env, CONTROLBOARD_BACKEND: `http://127.0.0.1:${MOCK_PORT}` },
      stdout: "ignore",
      stderr: "inherit",
    }),
  );
  await waitFor(`http://127.0.0.1:${MOCK_PORT}/healthz`);
  await waitFor(`http://127.0.0.1:${PREVIEW_PORT}/`);

  const browser = await chromium.launch({ executablePath: findChromium(), args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"] });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 }, deviceScaleFactor: 1 });
  shot = () => page.screenshot({ path: join(root, "smoke", "failure.png") });
  page.on("console", (m) => {
    if (m.type() === "error") errors.push(`console: ${m.text()}`);
  });
  page.on("pageerror", (e) => errors.push(`pageerror: ${e.message}`));

  await page.goto(`http://127.0.0.1:${PREVIEW_PORT}/`, { waitUntil: "networkidle" });
  await page.waitForSelector(".live__canvas canvas", { state: "attached", timeout: 15000 });
  await page.waitForSelector("[aria-label='Alerts']", { timeout: 15000 });
  await page.waitForSelector(".conn--open", { timeout: 15000 });
  // Start a scenario so the screenshot shows piles, weather and an alert.
  await fetch(`http://127.0.0.1:${MOCK_PORT}/api/experiments`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ scenario: "black-friday", params: { ordersPerSecond: 600 }, durationSec: 120, autoRecover: true }),
  });
  await fetch(`http://127.0.0.1:${MOCK_PORT}/api/alerts/test`, { method: "POST" });
  await page.waitForTimeout(9000);
  // Headless Chromium renders WebGL in software (SwiftShader) at a few fps on a busy machine. Advance
  // the live model's clock by ~4 s so the screenshot shows a representative frame (parcels in
  // flight) instead of the first frames; real browsers run this at 60 fps.
  await page.evaluate(() => {
    const lab = (window as unknown as { __kafkaLab?: { model: { step(ms: number): void } } }).__kafkaLab;
    return new Promise<void>((done) => {
      let n = 0;
      const tick = () => {
        for (let i = 0; i < 4; i++) lab?.model.step(25);
        if (++n < 40) setTimeout(tick, 60);
        else done();
      };
      tick();
    });
  });
  await page.waitForTimeout(2500);
  mkdirSync(join(root, "smoke"), { recursive: true });
  await page.screenshot({ path: join(root, "smoke", "screenshot.png") });
  if (process.env.SMOKE_EXTRA) {
    for (const tab of ["Experiments", "Services", "Tuning Lab", "Incident Drill", "Launchpad", "Scenarios"]) {
      await page.getByRole("tab", { name: tab }).click();
      await page.waitForTimeout(700);
      await page.screenshot({ path: join(root, "smoke", `tab-${tab.toLowerCase().replace(/\s+/g, "-")}.png`), fullPage: true });
    }
    await page.setViewportSize({ width: 768, height: 1000 });
    await page.waitForTimeout(800);
    await page.screenshot({ path: join(root, "smoke", "narrow-768.png"), fullPage: true });
  }
  await browser.close();
  if (errors.length) {
    failed = true;
    console.error(`smoke: ${errors.length} browser error(s):\n${errors.join("\n")}`);
  } else {
    console.log("smoke: OK — smoke/screenshot.png written, no console errors");
  }
} catch (e) {
  failed = true;
  console.error("smoke failed:", e instanceof Error ? e.message : e);
  if (errors.length) console.error(errors.join("\n"));
  await shot?.().catch(() => undefined);
} finally {
  cleanup();
}
process.exit(failed ? 1 : 0);
