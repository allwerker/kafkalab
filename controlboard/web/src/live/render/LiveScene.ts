/**
 * Procedural "neon logistics hub" renderer. Reads the LiveModel every frame and draws it with
 * Graphics (normal + additive glow layer) and a handful of Text objects. No image assets.
 */
import Phaser from "phaser";
import type { EventBus as Bus } from "../EventBus";
import type { LiveModel } from "../model/LiveModel";
import { BROKER_SIZE, COLORS, DLQ_POS, STATION_RADIUS, TRUCK_DOCK, WORLD } from "../model/layout";
import type { BrokerView, Effect, LaneView, StationView, Weather } from "../model/types";

const ACCENT: Record<string, number> = {
  "order-generator": COLORS.cyan,
  payment: COLORS.gold,
  inventory: COLORS.green,
  shipping: COLORS.magenta,
  notification: COLORS.blue,
  analytics: COLORS.violet,
};
const STATUS_COLOR = { ok: 0, degraded: 0xf59e0b, down: 0xef4444, paused: 0x7dd3fc, unknown: 0x64748b } as const;
const BG = 0x0a0d14;
const PANEL = 0x111723;
const FONT = "Inter Variable, Inter, system-ui, sans-serif";
const MONO = "JetBrains Mono, ui-monospace, monospace";

function hash(n: number): number {
  const x = Math.sin(n * 127.1 + 311.7) * 43758.5453;
  return x - Math.floor(x);
}

export class LiveScene extends Phaser.Scene {
  private bg!: Phaser.GameObjects.Graphics;
  private lanesG!: Phaser.GameObjects.Graphics;
  private glow!: Phaser.GameObjects.Graphics;
  private main!: Phaser.GameObjects.Graphics;
  private fx!: Phaser.GameObjects.Graphics;
  private stationText = new Map<string, { name: Phaser.GameObjects.Text; info: Phaser.GameObjects.Text }>();
  private brokerText: Phaser.GameObjects.Text[] = [];
  private dlqText!: Phaser.GameObjects.Text;
  private waitingText!: Phaser.GameObjects.Text;
  private lastInfo = 0;
  private lastFps = 0;
  private hoverKey = "";
  private bloom: Phaser.Filters.ParallelFilters | null = null;
  private slowFrames = 0;

  constructor(
    private readonly model: LiveModel,
    private readonly bus: typeof Bus,
    private readonly reducedMotion: boolean,
  ) {
    super({ key: "live" });
  }

  create(): void {
    // Static background is baked into a texture once; Graphics would re-tessellate it every frame.
    this.bg = this.make.graphics({}, false);
    this.drawBackground();
    this.bg.generateTexture("hub-bg", WORLD.width, WORLD.height);
    this.bg.destroy();
    this.add.image(0, 0, "hub-bg").setOrigin(0, 0);
    this.lanesG = this.add.graphics();
    this.glow = this.add.graphics().setBlendMode(Phaser.BlendModes.ADD);
    this.main = this.add.graphics();
    this.fx = this.add.graphics().setBlendMode(Phaser.BlendModes.ADD);

    const text = (x: number, y: number, s: string, size: number, color: string, font = FONT, weight = "600") =>
      this.add.text(x, y, s, { fontFamily: font, fontSize: `${size}px`, color, fontStyle: weight }).setOrigin(0.5);

    for (const st of this.model.stations) {
      // Notification and payment labels sit above the station: lanes and lag piles arrive from below.
      const above = st.id === "notification" || st.id === "payment";
      const y0 = above ? st.y - STATION_RADIUS - 52 : st.y + STATION_RADIUS + 20;
      this.stationText.set(st.id, {
        name: text(st.x, y0, st.label.toUpperCase(), 19, "#e2e8f0"),
        info: text(st.x, y0 + 22, "", 15, "#94a3b8", MONO, "400"),
      });
    }
    this.model.brokers.forEach((b) => this.brokerText.push(text(b.x, b.y + BROKER_SIZE.h / 2 + 16, `B${b.id}`, 14, "#94a3b8", MONO, "600")));
    text(822, 450 - BROKER_SIZE.h / 2 - 42, "KAFKA CLUSTER", 14, "#64748b", FONT, "700").setLetterSpacing?.(3);
    this.dlqText = text(DLQ_POS.x, DLQ_POS.y + 44, "DLQ", 15, "#fb7185", MONO, "600");
    this.waitingText = text(WORLD.width / 2, 120, "waiting for live data…", 16, "#64748b", MONO, "400");

    const lowFx = new URLSearchParams(window.location.search).get("fx") === "low";
    if (this.renderer.type === Phaser.WEBGL && !this.reducedMotion && !lowFx) {
      this.bloom = Phaser.Actions.AddEffectBloom(this.cameras.main, { threshold: 0.45, blurRadius: 3, blurSteps: 3, blendAmount: 0.55 })[0]?.parallelFilters ?? null;
    }

    this.input.on("pointermove", (p: Phaser.Input.Pointer) => this.onHover(p));
    this.input.on("pointerdown", (p: Phaser.Input.Pointer) => this.onClick(p));
    this.input.on("gameout", () => {
      this.hoverKey = "";
      this.bus.emit("hover", null);
    });
  }

  override update(time: number, delta: number): void {
    const m = this.model;
    // Adaptive quality: software GL / weak GPUs drop the full-screen bloom pass.
    if (this.bloom?.active) {
      this.slowFrames = delta > 33 ? this.slowFrames + 1 : Math.max(0, this.slowFrames - 1);
      if (this.slowFrames > 20) this.bloom.active = false;
    }
    m.step(delta);
    const t = m.time;
    this.waitingText.setVisible(!m.hasTick);
    this.drawLanes(t);
    this.glow.clear();
    this.main.clear();
    this.fx.clear();
    for (const s of m.stations) this.drawStation(s, t);
    for (const b of m.brokers) this.drawBroker(b, t);
    this.drawDlq(t);
    for (const lane of m.lanes) this.drawPile(lane);
    this.drawTrucks();
    this.drawParcels();
    for (const e of m.effects.live) this.drawEffect(e);
    for (const w of m.weather) this.drawWeather(w, t);
    // Labels refresh on wall-clock time (twice a second), independent of the model clock.
    if (time - this.lastInfo > 500) {
      this.lastInfo = time;
      this.updateTexts();
    }
    if (t - this.lastFps > 2000) {
      this.lastFps = t;
      this.bus.emit("fps", Math.round(this.game.loop.actualFps));
    }
  }

  // ------------------------------------------------------------------ static background

  private drawBackground(): void {
    const g = this.bg;
    g.clear();
    g.fillStyle(BG, 1).fillRect(0, 0, WORLD.width, WORLD.height);
    // floor grid
    g.lineStyle(1, 0x1a2233, 0.55);
    for (let x = 0; x <= WORLD.width; x += 40) g.lineBetween(x, 0, x, WORLD.height);
    for (let y = 0; y <= WORLD.height; y += 40) g.lineBetween(0, y, WORLD.width, y);
    // hub floor ring around the brokers
    g.lineStyle(1.5, 0x1e293b, 1).strokeCircle(822, 450, 125);
    g.lineStyle(1, 0x1e293b, 0.6).strokeCircle(822, 450, 160);
    // truck road
    g.fillStyle(0x0f1522, 1).fillRect(TRUCK_DOCK.x - 20, TRUCK_DOCK.y - 18, WORLD.width - TRUCK_DOCK.x + 40, 36);
    g.lineStyle(2, 0x334155, 0.8);
    for (let x = TRUCK_DOCK.x; x < WORLD.width; x += 34) g.lineBetween(x, TRUCK_DOCK.y, x + 16, TRUCK_DOCK.y);
    // station pads
    for (const s of this.model.stations) {
      g.fillStyle(PANEL, 1).fillCircle(s.x, s.y, STATION_RADIUS + 8);
      g.lineStyle(1, 0x223049, 1).strokeCircle(s.x, s.y, STATION_RADIUS + 8);
    }
  }

  // ------------------------------------------------------------------ lanes & piles

  private drawLanes(t: number): void {
    const g = this.lanesG;
    g.clear();
    for (const lane of this.model.lanes) {
      const pts = lane.path.points;
      g.lineStyle(1.4, lane.color, 0.14 + lane.activity * 0.35);
      g.beginPath();
      g.moveTo(pts[0]!.x, pts[0]!.y);
      for (let i = 1; i < pts.length; i++) g.lineTo(pts[i]!.x, pts[i]!.y);
      g.strokePath();
      // moving conveyor ticks
      if (!this.reducedMotion && lane.activity > 0.05) {
        const step = 0.1;
        const off = ((t / 2400) % step) / 1;
        g.fillStyle(lane.color, 0.35 * lane.activity);
        for (let f = off; f < 1; f += step) {
          const i = Math.floor(f * (pts.length - 1));
          const p = pts[i]!;
          g.fillRect(p.x - 1, p.y - 1, 2, 2);
        }
      }
    }
  }

  private drawPile(lane: LaneView): void {
    if (!lane.pile) return;
    const slots = this.model.pileSlots(lane);
    slots.forEach((s, i) => {
      const a = 0.9 - i * 0.03;
      this.main.fillStyle(lane.color, a * 0.85).fillRect(s.x - 3.5, s.y - 3.5, 7, 7);
      this.main.lineStyle(1, 0x0a0d14, 0.9).strokeRect(s.x - 3.5, s.y - 3.5, 7, 7);
    });
    if (lane.overflow) {
      const tail = slots[slots.length - 1]!;
      this.glow.fillStyle(0xf59e0b, 0.35).fillCircle(tail.x, tail.y, 6);
    }
  }

  // ------------------------------------------------------------------ stations

  private drawStation(s: StationView, t: number): void {
    const accent = ACCENT[s.id]!;
    const status = s.status;
    const flicker = status === "down" ? (hash(Math.floor(t / 90) + s.x) > 0.55 ? 1 : 0.25) : 1;
    const ring = status === "ok" ? accent : STATUS_COLOR[status];
    const pulse = status === "degraded" ? 0.6 + 0.4 * Math.sin(t / 220) : 1;
    const color = status === "down" ? 0x3f1d24 : status === "paused" ? 0x7dd3fc : accent;
    const dim = status === "down" ? 0.35 : status === "unknown" ? 0.45 : 1;

    // halo
    this.glow.fillStyle(ring, (0.1 + s.pulse * 0.18) * pulse * flicker).fillCircle(s.x, s.y, STATION_RADIUS + 14 + s.pulse * 6);
    this.main.lineStyle(2.5, ring, 0.9 * pulse * flicker).strokeCircle(s.x, s.y, STATION_RADIUS);
    this.main.fillStyle(0x0d1320, 0.96).fillCircle(s.x, s.y, STATION_RADIUS - 2);
    if (s.affected) {
      const r = STATION_RADIUS + 6 + 3 * Math.sin(t / 160);
      this.main.lineStyle(1.5, 0xf43f5e, 0.75).strokeCircle(s.x, s.y, r);
    }
    if (s.applying) {
      const a0 = (t / 300) % (Math.PI * 2);
      this.main.lineStyle(3, 0xfbbf24, 0.9).beginPath().arc(s.x, s.y, STATION_RADIUS + 4, a0, a0 + 1.2).strokePath();
    }

    const g = this.main;
    const a = dim * flicker;
    switch (s.id) {
      case "order-generator": {
        // factory: saw-tooth roof, chimney, door
        g.fillStyle(color, 0.22 * a).fillRect(s.x - 30, s.y - 10, 60, 32);
        g.lineStyle(2, color, a).strokeRect(s.x - 30, s.y - 10, 60, 32);
        g.beginPath().moveTo(s.x - 30, s.y - 10);
        for (let i = 0; i < 3; i++) g.lineTo(s.x - 30 + i * 20 + 20, s.y - 26).lineTo(s.x - 30 + i * 20 + 20, s.y - 10);
        g.strokePath();
        g.fillStyle(color, a).fillRect(s.x + 16, s.y - 38, 7, 16);
        g.fillStyle(color, 0.8 * a).fillRect(s.x - 8, s.y + 6, 16, 16);
        if (status === "ok" && !this.reducedMotion) {
          for (let i = 0; i < 3; i++) {
            const k = ((t / 1400 + i / 3) % 1);
            this.glow.fillStyle(color, 0.25 * (1 - k)).fillCircle(s.x + 19 + k * 8, s.y - 42 - k * 22, 3 + k * 5);
          }
        }
        break;
      }
      case "payment": {
        // vault door with spokes
        const rot = status === "ok" ? t / 2200 : 0;
        g.lineStyle(2.5, color, a).strokeCircle(s.x, s.y, 26);
        g.lineStyle(1.5, color, 0.6 * a).strokeCircle(s.x, s.y, 17);
        for (let i = 0; i < 6; i++) {
          const ang = rot + (i * Math.PI) / 3;
          g.lineBetween(s.x + Math.cos(ang) * 6, s.y + Math.sin(ang) * 6, s.x + Math.cos(ang) * 17, s.y + Math.sin(ang) * 17);
        }
        g.fillStyle(color, a).fillCircle(s.x, s.y, 5);
        g.fillStyle(color, 0.8 * a).fillRect(s.x + 26, s.y - 8, 5, 16);
        break;
      }
      case "inventory": {
        // warehouse racks
        g.lineStyle(2, color, a).strokeRect(s.x - 30, s.y - 26, 60, 50);
        for (let r = 0; r < 3; r++) {
          g.lineBetween(s.x - 30, s.y - 10 + r * 16, s.x + 30, s.y - 10 + r * 16);
          for (let c = 0; c < 5; c++) {
            const filled = hash(r * 7 + c + Math.floor(t / 4000)) > 0.3;
            if (filled) g.fillStyle(color, 0.65 * a).fillRect(s.x - 26 + c * 11, s.y - 22 + r * 16, 8, 9);
          }
        }
        break;
      }
      case "shipping": {
        // loading dock with three bay doors
        g.fillStyle(color, 0.14 * a).fillRect(s.x - 32, s.y - 22, 64, 44);
        g.lineStyle(2, color, a).strokeRect(s.x - 32, s.y - 22, 64, 44);
        for (let i = 0; i < 3; i++) {
          g.lineStyle(1.5, color, 0.8 * a).strokeRect(s.x - 26 + i * 19, s.y - 8, 14, 24);
          for (let k = 0; k < 3; k++) g.lineBetween(s.x - 26 + i * 19, s.y - 2 + k * 6, s.x - 12 + i * 19, s.y - 2 + k * 6);
        }
        g.fillStyle(color, a).fillTriangle(s.x - 36, s.y - 22, s.x + 36, s.y - 22, s.x, s.y - 36);
        break;
      }
      case "notification": {
        // antenna tower with radiating arcs
        g.lineStyle(2.5, color, a);
        g.lineBetween(s.x, s.y - 24, s.x - 16, s.y + 26);
        g.lineBetween(s.x, s.y - 24, s.x + 16, s.y + 26);
        g.lineBetween(s.x - 10, s.y + 6, s.x + 10, s.y + 6);
        g.lineBetween(s.x - 6, s.y - 8, s.x + 6, s.y - 8);
        g.fillStyle(color, a).fillCircle(s.x, s.y - 27, 5);
        for (let i = 1; i <= 2; i++) {
          g.lineStyle(1.5, color, 0.6 * a / i).beginPath().arc(s.x, s.y - 27, 9 * i + 3, -2.4, -0.7).strokePath();
          g.beginPath().arc(s.x, s.y - 27, 9 * i + 3, -2.4 - 1.8, -0.7 - 2.6).strokePath();
        }
        break;
      }
      case "analytics": {
        // big screen with a live bar chart of orders/s
        g.fillStyle(0x0b1020, 1).fillRoundedRect(s.x - 38, s.y - 30, 76, 50, 5);
        g.lineStyle(2, color, a).strokeRoundedRect(s.x - 38, s.y - 30, 76, 50, 5);
        g.lineBetween(s.x, s.y + 20, s.x, s.y + 30).lineBetween(s.x - 14, s.y + 30, s.x + 14, s.y + 30);
        const h = this.model.analytics.history.slice(-24);
        const max = Math.max(60, ...h);
        h.forEach((v, i) => {
          const bh = (v / max) * 34;
          g.fillStyle(color, 0.75 * a).fillRect(s.x - 34 + i * 2.85, s.y + 16 - bh, 2, bh);
        });
        break;
      }
    }
    if (status === "paused") {
      // frost: blue hexagon shell
      g.lineStyle(1.5, 0x7dd3fc, 0.8);
      g.beginPath();
      for (let i = 0; i <= 6; i++) {
        const ang = (i * Math.PI) / 3 + Math.PI / 6;
        const px = s.x + Math.cos(ang) * (STATION_RADIUS - 6);
        const py = s.y + Math.sin(ang) * (STATION_RADIUS - 6);
        if (i === 0) g.moveTo(px, py);
        else g.lineTo(px, py);
      }
      g.strokePath();
      this.glow.fillStyle(0x38bdf8, 0.12).fillCircle(s.x, s.y, STATION_RADIUS);
    }
    if (status === "down" && !this.reducedMotion && hash(Math.floor(t / 70) + s.y) > 0.8) {
      this.fx.lineStyle(1.5, 0xf87171, 0.9).lineBetween(s.x - 20, s.y - 30, s.x - 8, s.y - 18);
    }
  }

  // ------------------------------------------------------------------ brokers

  private drawBroker(b: BrokerView, t: number): void {
    const { w, h } = BROKER_SIZE;
    const x = b.x - w / 2;
    const y = b.y - h / 2;
    const g = this.main;
    if (b.underReplicated > 0 && b.up) {
      this.glow.fillStyle(0xf59e0b, 0.16 + 0.1 * Math.sin(t / 200)).fillRoundedRect(x - 8, y - 8, w + 16, h + 16, 10);
    } else if (b.up) {
      this.glow.fillStyle(COLORS.cyan, 0.06).fillRoundedRect(x - 6, y - 6, w + 12, h + 12, 10);
    }
    g.fillStyle(b.up ? 0x111a2b : 0x160d12, 1).fillRoundedRect(x, y, w, h, 6);
    g.lineStyle(2, b.up ? (b.underReplicated ? 0xf59e0b : 0x38bdf8) : 0x7f1d1d, b.up ? 0.9 : 0.6 + 0.4 * hash(Math.floor(t / 80) + b.id)).strokeRoundedRect(x, y, w, h, 6);
    // 4 x 6 leader LEDs
    for (let i = 0; i < 24; i++) {
      const cx = x + 10 + (i % 4) * 10;
      const cy = y + 14 + Math.floor(i / 4) * 12;
      const lit = b.up && i < b.leaders;
      const blink = lit && hash(i * 13 + Math.floor(t / 260) + b.id * 5) > 0.85;
      g.fillStyle(lit ? (blink ? 0xffffff : 0x22d3ee) : 0x1e293b, lit ? 0.95 : 1).fillRect(cx - 2.5, cy - 2, 5, 4);
      if (lit && !blink) this.glow.fillStyle(0x22d3ee, 0.18).fillCircle(cx, cy, 4);
    }
    if (b.controller && b.up) {
      const cy = y - 14;
      g.fillStyle(0xfbbf24, 1).fillTriangle(b.x - 11, cy + 6, b.x - 11, cy - 4, b.x - 4, cy + 1);
      g.fillTriangle(b.x - 6, cy + 6, b.x, cy - 7, b.x + 6, cy + 6);
      g.fillTriangle(b.x + 11, cy + 6, b.x + 11, cy - 4, b.x + 4, cy + 1);
      g.fillRect(b.x - 11, cy + 5, 22, 4);
      this.glow.fillStyle(0xfbbf24, 0.3).fillCircle(b.x, cy, 12);
    }
    if (!b.up && !this.reducedMotion && b.downForMs >= 0) {
      for (let i = 0; i < 5; i++) {
        const k = hash(Math.floor(t / 60) * 7 + i + b.id * 31);
        if (k > 0.55) {
          const sx = x + k * w;
          const sy = y + hash(k * 99) * 20;
          this.fx.lineStyle(1.5, 0xfbbf24, 0.9).lineBetween(sx, sy, sx + (k - 0.5) * 18, sy - 10 - k * 10);
        }
      }
    }
  }

  private drawDlq(t: number): void {
    const { x, y } = DLQ_POS;
    this.glow.fillStyle(0xf43f5e, 0.12 + 0.05 * Math.sin(t / 400)).fillEllipse(x, y, 120, 46);
    this.main.fillStyle(0x07090f, 1).fillEllipse(x, y, 92, 32);
    this.main.lineStyle(2, 0xf43f5e, 0.85).strokeEllipse(x, y, 92, 32);
    this.main.lineStyle(1, 0xf43f5e, 0.35).strokeEllipse(x, y + 4, 70, 20);
  }

  // ------------------------------------------------------------------ moving things

  private drawParcels(): void {
    for (const p of this.model.parcels.live) {
      const s = p.size;
      this.glow.fillStyle(p.color, 0.28 * p.alpha).fillCircle(p.x, p.y, s + 2);
      this.main.fillStyle(p.color, p.alpha).fillRoundedRect(p.x - s / 2, p.y - s / 2, s, s, 1.5);
    }
  }

  private drawTrucks(): void {
    for (const tr of this.model.trucks.live) {
      const { x, y } = tr;
      const g = this.main;
      g.fillStyle(0x1f1630, 1).fillRoundedRect(x - 30, y - 11, 40, 22, 3);
      g.lineStyle(1.5, COLORS.magenta, 0.95).strokeRoundedRect(x - 30, y - 11, 40, 22, 3);
      for (let i = 0; i < Math.min(3, tr.load); i++) g.fillStyle(COLORS.magenta, 0.8).fillRect(x - 26 + i * 12, y - 6, 9, 12);
      g.fillStyle(0x2a1f3d, 1).fillRoundedRect(x + 11, y - 9, 16, 18, 4);
      g.lineStyle(1.5, COLORS.magenta, 0.95).strokeRoundedRect(x + 11, y - 9, 16, 18, 4);
      g.fillStyle(0x0a0d14, 1).fillCircle(x - 20, y + 12, 4).fillCircle(x + 18, y + 12, 4);
      const head = tr.state === "departing" ? 0.9 : 0.4;
      this.glow.fillStyle(0xfef08a, head * 0.6).fillTriangle(x + 27, y - 4, x + 70, y - 14, x + 70, y + 10);
      if (tr.state === "departing") this.glow.fillStyle(COLORS.magenta, 0.25).fillRect(x - 60, y - 2, 28, 4);
    }
  }

  private drawEffect(e: Effect): void {
    const k = e.age / e.ttl;
    const g = this.fx;
    const a = 1 - k;
    switch (e.kind) {
      case "coin": {
        const y = e.y - 30 - k * 40;
        g.fillStyle(COLORS.gold, a).fillCircle(e.x + 20, y, 6);
        g.lineStyle(1.5, 0xfff7cc, a);
        for (let i = 0; i < 4; i++) {
          const ang = (i * Math.PI) / 2 + k * 3;
          g.lineBetween(e.x + 20 + Math.cos(ang) * 9, y + Math.sin(ang) * 9, e.x + 20 + Math.cos(ang) * (13 + k * 6), y + Math.sin(ang) * (13 + k * 6));
        }
        break;
      }
      case "burst": {
        g.lineStyle(2.5, COLORS.red, a).strokeCircle(e.x, e.y, 20 + k * 40);
        for (let i = 0; i < 8; i++) {
          const ang = (i * Math.PI) / 4 + e.seed;
          g.lineBetween(e.x + Math.cos(ang) * (22 + k * 30), e.y + Math.sin(ang) * (22 + k * 30), e.x + Math.cos(ang) * (30 + k * 46), e.y + Math.sin(ang) * (30 + k * 46));
        }
        break;
      }
      case "cross": {
        const s = 10 + k * 8;
        const cx = e.x + 34;
        const cy = e.y - 34;
        g.lineStyle(4, COLORS.orange, a).lineBetween(cx - s, cy - s, cx + s, cy + s).lineBetween(cx - s, cy + s, cx + s, cy - s);
        break;
      }
      case "wave": {
        for (let i = 0; i < 3; i++) {
          const kk = (k + i * 0.2) % 1;
          g.lineStyle(2, COLORS.blue, (1 - kk) * 0.8).beginPath().arc(e.x, e.y - 27, 14 + kk * 70, -2.6, -0.55).strokePath();
        }
        break;
      }
      case "coinBack": {
        const x = Phaser.Math.Linear(e.x, e.tx, k);
        const y = Phaser.Math.Linear(e.y, e.ty, k) - Math.sin(k * Math.PI) * 120;
        g.fillStyle(COLORS.gold, 0.95).fillCircle(x, y, 6);
        g.fillStyle(COLORS.gold, 0.25).fillCircle(x, y, 12);
        break;
      }
      case "release": {
        const y = e.y - 20 - k * 46;
        g.lineStyle(2, COLORS.green, a).strokeRect(e.x - 30 - 6, y - 6, 12, 12);
        break;
      }
      case "splash": {
        g.lineStyle(2, COLORS.red, a).strokeEllipse(e.x, e.y, 30 + k * 80, 10 + k * 24);
        break;
      }
      case "spark":
        g.fillStyle(e.color, a).fillCircle(e.x, e.y, 2);
        break;
    }
  }

  // ------------------------------------------------------------------ chaos weather

  private targetPos(target: string): { x: number; y: number } | null {
    if (target.startsWith("broker-")) {
      const b = this.model.brokers[Number(target.slice(7))];
      return b ? { x: b.x, y: b.y } : null;
    }
    const s = this.model.stations.find((st) => st.id === target);
    return s ? { x: s.x, y: s.y } : null;
  }

  private drawWeather(w: Weather, t: number): void {
    const g = this.fx;
    const rm = this.reducedMotion;
    switch (w.kind) {
      case "rush": {
        for (let i = 0; i < (rm ? 8 : 26); i++) {
          const y = 300 + hash(i) * 300;
          const speed = 0.4 + hash(i + 50) * 0.6;
          const x = ((t * speed + hash(i + 9) * 900) % 900) - 120;
          g.lineStyle(1.5, COLORS.cyan, 0.35).lineBetween(x, y, x + 40 + speed * 40, y);
        }
        break;
      }
      case "glitch": {
        for (const target of w.targets) {
          const p = this.targetPos(target);
          if (!p) continue;
          const frame = Math.floor(t / (rm ? 400 : 80));
          for (let i = 0; i < 6; i++) {
            if (hash(frame * 11 + i) < 0.45) continue;
            const yy = p.y - 60 + hash(frame + i * 3) * 120;
            const ww = 30 + hash(frame * 3 + i) * 90;
            g.fillStyle(i % 2 ? COLORS.magenta : COLORS.cyan, 0.22).fillRect(p.x - 60 + hash(i + frame) * 40, yy, ww, 3);
          }
        }
        break;
      }
      case "lightning": {
        for (const target of w.targets) {
          const p = this.targetPos(target);
          if (!p) continue;
          const cycle = t % 1500;
          if (rm || cycle > 220) continue;
          const seed = Math.floor(t / 1500) + p.x;
          let x = p.x + (hash(seed) - 0.5) * 60;
          let y = 0;
          g.lineStyle(3, 0xe0f2fe, 1 - cycle / 220);
          g.beginPath().moveTo(x, y);
          while (y < p.y - 50) {
            y += 30 + hash(seed + y) * 30;
            x += (hash(seed + y * 3) - 0.5) * 50;
            g.lineTo(x, Math.min(y, p.y - 50));
          }
          g.strokePath();
          g.fillStyle(0xe0f2fe, 0.06 * (1 - cycle / 220)).fillRect(0, 0, WORLD.width, WORLD.height);
        }
        break;
      }
      case "smoke": {
        for (const target of w.targets) {
          const p = this.targetPos(target);
          if (!p) continue;
          for (let i = 0; i < (rm ? 3 : 8); i++) {
            const k = (t / 2600 + i / 8) % 1;
            const x = p.x - 20 + hash(i) * 40 + k * 18;
            const y = p.y - 40 - k * 90;
            this.main.fillStyle(0x64748b, 0.22 * (1 - k)).fillCircle(x, y, 8 + k * 16);
          }
        }
        break;
      }
      case "matrix": {
        for (let c = 0; c < (rm ? 10 : 34); c++) {
          const x = 20 + hash(c) * (WORLD.width - 40);
          const speed = 0.06 + hash(c + 7) * 0.08;
          const head = (t * speed + hash(c + 3) * 900) % (WORLD.height + 200);
          for (let k = 0; k < 8; k++) {
            const y = head - k * 14;
            if (y < 0 || y > WORLD.height) continue;
            g.fillStyle(0x4ade80, (k === 0 ? 0.6 : 0.3) * (1 - k / 8)).fillRect(x, y, 4, 8);
          }
        }
        break;
      }
      case "fog": {
        for (let i = 0; i < 6; i++) {
          const x = ((t / 40 + hash(i) * WORLD.width) % (WORLD.width + 300)) - 150;
          const y = 120 + hash(i + 5) * 660;
          this.main.fillStyle(0x6d28d9, 0.05).fillCircle(x, y, 120 + hash(i + 2) * 80);
        }
        break;
      }
    }
  }

  // ------------------------------------------------------------------ texts & interaction

  /** Text re-rasterisation is expensive: only touch a Text when its content or colour changed. */
  private setT(t: Phaser.GameObjects.Text, text: string, color?: string): void {
    if (t.text !== text) t.setText(text);
    if (color && t.style.color !== color) t.setColor(color);
  }

  private updateTexts(): void {
    const m = this.model;
    for (const s of m.stations) {
      const tx = this.stationText.get(s.id)!;
      const status = s.status === "ok" ? "" : ` · ${s.status.toUpperCase()}`;
      const lag = s.id === "order-generator" || s.lag === 0 ? "" : ` · lag ${s.lag >= 1000 ? `${(s.lag / 1000).toFixed(1)}k` : s.lag}`;
      this.setT(tx.info, `${s.rate.toFixed(0)}/s · ${s.ready}/${s.replicas} pods${lag}${status}`, s.status === "down" ? "#f87171" : s.status === "degraded" ? "#fbbf24" : s.status === "paused" ? "#7dd3fc" : "#94a3b8");
    }
    m.brokers.forEach((b, i) =>
      this.setT(this.brokerText[i]!, `B${b.id}${b.up ? "" : " DOWN"}${b.underReplicated ? ` · URP ${b.underReplicated}` : ""}`, b.up ? (b.underReplicated ? "#fbbf24" : "#94a3b8") : "#f87171"),
    );
    this.setT(this.dlqText, `DLQ · ${m.dlqCount}`);
  }

  private onHover(p: Phaser.Input.Pointer): void {
    const hit = this.model.hitTest(p.worldX, p.worldY);
    this.input.setDefaultCursor(hit ? "pointer" : "default");
    if (!hit) {
      if (this.hoverKey) this.bus.emit("hover", null);
      this.hoverKey = "";
      return;
    }
    const tip = this.model.describe(hit);
    this.hoverKey = tip.title;
    const canvas = this.game.canvas;
    const rect = canvas.getBoundingClientRect();
    const parent = canvas.parentElement!.getBoundingClientRect();
    const sx = rect.width / WORLD.width;
    const sy = rect.height / WORLD.height;
    this.bus.emit("hover", { title: tip.title, lines: tip.lines, x: rect.left - parent.left + p.worldX * sx, y: rect.top - parent.top + p.worldY * sy });
  }

  private onClick(p: Phaser.Input.Pointer): void {
    const hit = this.model.hitTest(p.worldX, p.worldY);
    if (!hit) return;
    if (hit.type === "parcel") this.bus.emit("select-order", { orderId: hit.parcel.orderId });
    else if (hit.type === "station") this.bus.emit("select-station", { id: hit.station.id });
    else if (hit.type === "broker") this.bus.emit("select-broker", { id: hit.broker.id });
  }
}

