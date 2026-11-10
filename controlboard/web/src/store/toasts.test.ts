import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it } from "vitest";
import { ApiError } from "../api/client";
import { MAX_TOASTS, describeError, toast, toastError, toastStore, useToasts } from "./toasts";

beforeEach(() => toastStore.setState({ toasts: [] }));

describe("toasts", () => {
  it("shouldKeepOnlyTheNewestToastsWhenManyAppear", () => {
    // GIVEN: an operator clicking very fast
    // WHEN: more toasts than fit are pushed
    for (let i = 0; i < MAX_TOASTS + 2; i++) toast("info", `t${i}`);
    // THEN: only the newest stay on screen
    const titles = toastStore.getState().toasts.map((t) => t.title);
    expect(titles).toEqual(["t2", "t3", "t4", "t5"]);
  });

  it("shouldRemoveAToastWhenItIsDismissed", () => {
    // GIVEN: a success toast with details
    const id = toast("success", "Done", "all good");
    expect(toastStore.getState().toasts[0]!.detail).toBe("all good");
    // WHEN: it is dismissed
    toastStore.getState().dismiss(id);
    // THEN: it is gone
    expect(toastStore.getState().toasts).toEqual([]);
  });

  it("shouldExplainContractErrorsInPlainWordsWhenTheApiRefuses", () => {
    // GIVEN: the documented 409/400 errors, an unknown code, a network error and a random exception
    // WHEN: they are described
    // THEN: the operator sees friendly titles plus the server message
    expect(describeError(new ApiError(409, "chaos_disarmed", "Arm first"))).toEqual({ title: "Chaos is disarmed", detail: "Arm first" });
    expect(describeError(new ApiError(409, "broker_fault_active", "x")).title).toBe("A broker fault is already active");
    expect(describeError(new ApiError(400, "confirmation_required", "x")).title).toBe("Confirmation required");
    expect(describeError(new ApiError(0, "network_error", "x")).title).toBe("Backend unreachable");
    expect(describeError(new ApiError(0, "weird", "x")).title).toBe("Request failed (network)");
    expect(describeError(new ApiError(418, "teapot", "x")).title).toBe("Request failed (418)");
    expect(describeError(new Error("boom"))).toEqual({ title: "Unexpected error", detail: "Error: boom" });
  });

  it("shouldShowAnErrorToastWhenAnActionFails", () => {
    // GIVEN: a refused broker scenario
    // WHEN: the error is toasted
    toastError(new ApiError(409, "broker_fault_active", "Stop the other one first"));
    // THEN: a red toast explains it
    expect(toastStore.getState().toasts[0]).toMatchObject({ tone: "error", title: "A broker fault is already active", detail: "Stop the other one first" });
  });

  it("shouldRenderNewToastsWhenComponentsSubscribe", () => {
    // GIVEN: a component listening for toasts
    const { result } = renderHook(() => useToasts());
    // WHEN: a toast is pushed
    act(() => void toast("warning", "Careful"));
    // THEN: the component receives it
    expect(result.current.map((t) => t.title)).toEqual(["Careful"]);
  });
});
