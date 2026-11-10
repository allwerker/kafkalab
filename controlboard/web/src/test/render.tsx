/** Render helper: board store reset + injected fake API. */
import { render } from "@testing-library/react";
import type { ReactElement } from "react";
import { ApiContext } from "../components/context";
import { initialBoard, type BoardData } from "../store/reducer";
import { boardStore } from "../store/store";
import { toastStore } from "../store/toasts";
import { fakeApi, type FakeApi } from "./fakeApi";

export function seedBoard(over: Partial<BoardData> = {}): void {
  boardStore.setState({ drill: undefined, retryInMs: undefined, ...initialBoard(), ...over });
  toastStore.setState({ toasts: [] });
}

export function renderWithApi(ui: ReactElement, api: FakeApi = fakeApi()) {
  return { api, ...render(<ApiContext.Provider value={api}>{ui}</ApiContext.Provider>) };
}

export function toastTitles(): string[] {
  return toastStore.getState().toasts.map((t) => t.title);
}
