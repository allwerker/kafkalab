/**
 * Per-frame CPU profile of the live view: wraps the scene's update() (our drawing code) and the
 * renderer's render() (Phaser's WebGL submission) and reports average milliseconds per frame.
 * Independent of how fast the (software) GPU rasterises. Usage: BOARD_URL=... bun run scripts/frame-profile.ts
 */
import { chromium } from "playwright-core";

const url = process.env.BOARD_URL ?? "http://board.localhost:8080/";
const browser = await chromium.launch({
  executablePath: process.env.CHROME_PATH ?? "/usr/bin/chromium",
  args: ["--use-angle=swiftshader", "--enable-unsafe-swiftshader", "--ignore-gpu-blocklist"],
});
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
await page.goto(url, { waitUntil: "domcontentloaded" });
await page.waitForTimeout(8000);
const result = await page.evaluate(async () => {
  type Fn = (...a: unknown[]) => void;
  type Lab = { game: { scene: { scenes: { sys: { sceneUpdate: Fn } }[] }; loop: { callback: Fn } } };
  const lab = (globalThis as unknown as { __kafkaLab: Lab }).__kafkaLab;
  const sys = lab.game.scene.scenes[0]!.sys;
  const loop = lab.game.loop;
  let upd = 0, total = 0, frames = 0;
  const u0 = sys.sceneUpdate;
  sys.sceneUpdate = function (this: unknown, ...a: unknown[]) { const s = performance.now(); u0.apply(this, a); upd += performance.now() - s; };
  const c0 = loop.callback;
  loop.callback = (...a: unknown[]) => { const s = performance.now(); c0(...a); total += performance.now() - s; frames++; };
  await new Promise((r) => setTimeout(r, 15000));
  return { frames, update: upd / frames, render: (total - upd) / frames };
});
console.log(`frames ${result.frames} · update ${result.update.toFixed(2)} ms/frame · render+rest ${result.render.toFixed(2)} ms/frame`);
await browser.close();
