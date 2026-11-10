import { createContext, useContext, useEffect, useState } from "react";
import { api as defaultApi, type Api } from "../api/client";
import { useBoard } from "../store/store";

export const ApiContext = createContext<Api>(defaultApi);

export function useApi(): Api {
  return useContext(ApiContext);
}

/** Server-aligned "now" in ms, re-rendering every `intervalMs`. */
export function useNow(intervalMs = 1000): number {
  const offset = useBoard((b) => b.clockOffsetMs);
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return now + offset;
}
