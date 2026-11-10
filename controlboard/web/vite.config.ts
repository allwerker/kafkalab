/// <reference types="vitest/config" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backend = process.env.CONTROLBOARD_BACKEND ?? "http://127.0.0.1:8090";

// 100% line+branch coverage for pure code, >= 90% lines for components.
// Exclusions are documented with reasons in COVERAGE.md.
const pure = { lines: 100, branches: 100, functions: 100, statements: 100 };

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    chunkSizeWarningLimit: 2000,
    rolldownOptions: {
      output: {
        advancedChunks: {
          groups: [{ name: "phaser", test: /node_modules[\\/]phaser/ }],
        },
      },
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/api": { target: backend, ws: true, changeOrigin: true },
      "/healthz": { target: backend },
    },
  },
  preview: {
    proxy: {
      "/api": { target: backend, ws: true, changeOrigin: true },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test/setup.ts"],
    include: ["src/**/*.test.{ts,tsx}"],
    restoreMocks: true,
    coverage: {
      provider: "v8",
      reporter: ["text-summary", "text", "json-summary"],
      include: ["src/**/*.{ts,tsx}"],
      exclude: [
        "src/**/*.test.{ts,tsx}",
        "src/test/**",
        "src/main.tsx",
        "src/live/render/**",
        "src/api/types.ts",
      ],
      thresholds: {
        "src/live/model/**": pure,
        "src/api/**": pure,
        "src/store/**": pure,
        "src/lib/**": pure,
        "src/components/**": { lines: 90 },
      },
    },
  },
});
