/** Transient toast messages (errors from the API, confirmations). */
import { createStore } from "zustand/vanilla";
import { useStore } from "zustand";
import { ApiError } from "../api/client";

export type ToastTone = "info" | "success" | "warning" | "error";

export interface Toast {
  id: number;
  tone: ToastTone;
  title: string;
  detail?: string;
}

interface ToastState {
  toasts: Toast[];
  push(t: Omit<Toast, "id">): number;
  dismiss(id: number): void;
}

let counter = 0;
export const MAX_TOASTS = 4;

export const toastStore = createStore<ToastState>()((set) => ({
  toasts: [],
  push: (t) => {
    const id = ++counter;
    set((s) => ({ toasts: [...s.toasts, { ...t, id }].slice(-MAX_TOASTS) }));
    return id;
  },
  dismiss: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}));

export function toast(tone: ToastTone, title: string, detail?: string): number {
  return toastStore.getState().push(detail === undefined ? { tone, title } : { tone, title, detail });
}

/** Human-friendly title for API errors (409 / 400 codes from the contract). */
export function describeError(e: unknown): { title: string; detail: string } {
  if (e instanceof ApiError) {
    const titles: Record<string, string> = {
      chaos_disarmed: "Chaos is disarmed",
      broker_fault_active: "A broker fault is already active",
      confirmation_required: "Confirmation required",
      network_error: "Backend unreachable",
    };
    return { title: titles[e.code] ?? `Request failed (${e.status || "network"})`, detail: e.message };
  }
  return { title: "Unexpected error", detail: String(e) };
}

export function toastError(e: unknown): number {
  const { title, detail } = describeError(e);
  return toast("error", title, detail);
}

export function useToasts(): Toast[] {
  return useStore(toastStore, (s) => s.toasts);
}
