/**
 * Renders the README screenshots from the running stack into docs/images/.
 * Usage (stack running): bun run readme-shots
 */
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { chromium, type Page } from "playwright-core";

const out = join(import.meta.dir, "..", "..", "..", "docs", "images");
mkdirSync(out, { recursive: true });
const board = process.env.BOARD_URL ?? "http://board.localhost:8080/";
const grafana = process.env.GRAFANA_URL ?? "http://grafana.localhost:8080";

const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH ?? "/usr/bin/chromium",
  args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"],
});

async function shot(page: Page, name: string): Promise<void> {
  await page.screenshot({ path: join(out, name) });
  console.log(`wrote docs/images/${name}`);
}

// Controlboard: live view, alert panel and the tabs below it.
const page = await browser.newPage({ viewport: { width: 1600, height: 1000 } });
await page.goto(board, { waitUntil: "domcontentloaded" });
await page.waitForSelector(".live__canvas canvas", { state: "attached", timeout: 30000 });
await page.waitForTimeout(15000);
await shot(page, "controlboard-overview.png");
for (const [tab, file] of [["Launchpad", "controlboard-launchpad.png"]] as const) {
  await page.getByRole("tab", { name: new RegExp(tab) }).first().click().catch(async () => page.getByText(tab, { exact: false }).first().click());
  await page.waitForTimeout(2500);
  await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  await page.waitForTimeout(1000);
  await shot(page, file);
  await page.evaluate(() => window.scrollTo(0, 0));
}

// Grafana dashboards (anonymous viewer, kiosk mode).
const g = await browser.newPage({ viewport: { width: 1600, height: 1100 } });
for (const [uid, file] of [["kafka-overview", "grafana-kafka-overview.png"], ["kafka-lag", "grafana-consumer-lag.png"]] as const) {
  // Auto-refresh keeps the network busy, so wait for rendered panels instead of network idle.
  await g.goto(`${grafana}/d/${uid}?orgId=1&from=now-1h&to=now&kiosk`, { waitUntil: "domcontentloaded" });
  await g.waitForSelector(".react-grid-item", { timeout: 60000 });
  await g.waitForTimeout(20000);
  await shot(g, file);
}
await browser.close();
