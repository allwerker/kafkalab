/**
 * Phaser bootstrap for the live view (thin render layer; excluded from coverage, see COVERAGE.md).
 * The pure LiveModel lives in ../model; this file only wires the EventBus and the game loop.
 */
import Phaser from "phaser";
import type { EventBus as Bus } from "../EventBus";
import { LiveModel } from "../model/LiveModel";
import { WORLD } from "../model/layout";
import { LiveScene } from "./LiveScene";

export function startLiveGame(parent: HTMLElement, bus: typeof Bus): { destroy(): void } {
  const model = new LiveModel();
  const reducedMotion = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
  const scene = new LiveScene(model, bus, reducedMotion);
  const game = new Phaser.Game({
    type: new URLSearchParams(window.location.search).get("renderer") === "canvas" ? Phaser.CANVAS : Phaser.AUTO,
    parent,
    width: WORLD.width,
    height: WORLD.height,
    backgroundColor: "#0a0d14",
    antialias: true,
    banner: false,
    fps: { target: 60, smoothStep: true },
    scale: { mode: Phaser.Scale.FIT, autoCenter: Phaser.Scale.CENTER_BOTH },
    scene,
  });

  // Debug/smoke hook: inspect the model from the browser console.
  (window as unknown as { __kafkaLab?: unknown }).__kafkaLab = { model, game };

  const offs = [
    bus.on("tick", (t) => model.applyTick(t)),
    bus.on("events", (b) => {
      // While the tab is hidden the loop sleeps; dropping events keeps the scene from flooding on return.
      if (!document.hidden) model.applyEvents(b);
    }),
    bus.on("experiments", ({ experiments, categories }) => model.applyExperiments(experiments, categories)),
  ];

  const onVisibility = () => {
    if (document.hidden) game.loop.sleep();
    else game.loop.wake();
  };
  document.addEventListener("visibilitychange", onVisibility);

  return {
    destroy() {
      offs.forEach((off) => off());
      document.removeEventListener("visibilitychange", onVisibility);
      bus.emit("hover", null);
      game.destroy(true);
    },
  };
}
