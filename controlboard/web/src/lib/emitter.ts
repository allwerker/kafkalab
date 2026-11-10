/** Minimal typed event emitter (used as the React ↔ Phaser EventBus). */
export class Emitter<Events extends Record<string, unknown>> {
  private handlers = new Map<keyof Events, Set<(payload: never) => void>>();

  on<K extends keyof Events>(type: K, fn: (payload: Events[K]) => void): () => void {
    let set = this.handlers.get(type);
    if (!set) this.handlers.set(type, (set = new Set()));
    set.add(fn as (payload: never) => void);
    return () => this.off(type, fn);
  }

  off<K extends keyof Events>(type: K, fn: (payload: Events[K]) => void): void {
    this.handlers.get(type)?.delete(fn as (payload: never) => void);
  }

  emit<K extends keyof Events>(type: K, payload: Events[K]): void {
    for (const fn of this.handlers.get(type) ?? []) (fn as (p: Events[K]) => void)(payload);
  }

  listenerCount(type: keyof Events): number {
    return this.handlers.get(type)?.size ?? 0;
  }
}
