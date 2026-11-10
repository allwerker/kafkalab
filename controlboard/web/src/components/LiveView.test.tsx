import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { EventBus } from "../live/EventBus";
import { experiment, scenario, service } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard } from "../test/render";
import { LiveView, loadRenderer, type StartLiveGame } from "./LiveView";

function fakeGame() {
  const destroy = vi.fn();
  const start = vi.fn<StartLiveGame>(() => ({ destroy }));
  return { start, destroy, load: () => Promise.resolve(start as StartLiveGame) };
}

describe("LiveView", () => {
  beforeEach(() => seedBoard({ hasState: true, eventScale: 40, services: [service()] }));

  it("shouldStartTheRendererInsideTheHeroAreaAndDestroyItWhenUnmounted", async () => {
    // GIVEN: the Phaser render layer can be loaded
    const g = fakeGame();
    // WHEN: the live view mounts and later unmounts
    const { unmount } = renderWithApi(<LiveView load={g.load} />);
    await waitFor(() => expect(g.start).toHaveBeenCalledWith(screen.getByTestId("live-canvas-host"), EventBus));
    unmount();
    // THEN: the game is torn down and no listeners leak
    expect(g.destroy).toHaveBeenCalled();
    expect(EventBus.listenerCount("hover")).toBe(0);
  });

  it("shouldShowTheAdaptiveScaleWhenEventsAreSampled", () => {
    // GIVEN: each sampled parcel stands for 40 real orders
    // WHEN: the live view renders
    renderWithApi(<LiveView load={fakeGame().load} />);
    // THEN: the legend explains the scale and lists the event kinds
    expect(screen.getByTestId("scale-label")).toHaveTextContent("1 parcel = 40 orders");
    expect(screen.getByText("Dead-lettered")).toBeInTheDocument();
  });

  it("shouldUseSingularWhenOneParcelIsOneOrder", () => {
    // GIVEN: traffic is low enough that no sampling happens
    seedBoard({ eventScale: 1 });
    // WHEN: the live view renders
    renderWithApi(<LiveView load={fakeGame().load} />);
    // THEN: the label reads "1 order"
    expect(screen.getByTestId("scale-label")).toHaveTextContent("1 parcel = 1 order");
  });

  it("shouldExplainWhenTheRendererCannotBeLoaded", async () => {
    // GIVEN: the WebGL bundle fails to load
    // WHEN: the live view mounts
    renderWithApi(<LiveView load={() => Promise.reject(new Error("chunk missing"))} />);
    // THEN: a readable error replaces the scene
    expect(await screen.findByText(/Live view unavailable: chunk missing/)).toBeInTheDocument();
  });

  it("shouldShowATooltipWhenTheSceneReportsAHover", () => {
    // GIVEN: the scene is running
    renderWithApi(<LiveView load={fakeGame().load} />);
    // WHEN: the pointer hovers a parcel and then leaves it
    act(() => EventBus.emit("hover", { title: "Order 0c7c2c37", lines: ["Order created", "orders.created · partition 3"], x: 100, y: 50 }));
    expect(screen.getByRole("tooltip")).toHaveTextContent("Order 0c7c2c37");
    expect(screen.getByRole("tooltip")).toHaveStyle({ left: "114px", top: "64px" });
    act(() => EventBus.emit("hover", null));
    // THEN: the tooltip follows and disappears again
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });

  it("shouldOpenTheOrderTimelineWhenAParcelIsClicked", async () => {
    // GIVEN: the scene is running and the backend knows the order
    const api = fakeApi({ order: vi.fn().mockResolvedValue({ orderId: "o-1", events: [] }) });
    renderWithApi(<LiveView load={fakeGame().load} />, api);
    // WHEN: the operator clicks a parcel of order o-1
    act(() => EventBus.emit("select-order", { orderId: "o-1" }));
    // THEN: the order drawer opens and can be closed again
    expect(await screen.findByRole("dialog", { name: "Order o-1" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Close order" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shouldOpenTheSideCardWhenAStationOrBrokerIsClicked", async () => {
    // GIVEN: the scene is running
    renderWithApi(<LiveView load={fakeGame().load} />);
    // WHEN: the operator clicks the payment station, then broker 2
    act(() => EventBus.emit("select-station", { id: "payment" }));
    expect(screen.getByRole("dialog", { name: "Payment" })).toBeInTheDocument();
    act(() => EventBus.emit("select-broker", { id: 2 }));
    // THEN: the side card switches to the broker and closes on request
    expect(screen.getByRole("dialog", { name: "Broker 2" })).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: "Close" }));
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("shouldShowFpsWhenTheSceneReportsIt", () => {
    // GIVEN: the scene is running
    renderWithApi(<LiveView load={fakeGame().load} />);
    // WHEN: the scene reports its frame rate
    act(() => EventBus.emit("fps", 58));
    // THEN: the HUD shows it
    expect(screen.getByText("58 fps")).toBeInTheDocument();
  });

  it("shouldForwardExperimentsWithCategoriesWhenTheyChange", () => {
    // GIVEN: Black Friday (a traffic scenario) is running
    seedBoard({ experiments: [experiment()], scenarios: [scenario()] });
    const seen = vi.fn();
    const off = EventBus.on("experiments", seen);
    // WHEN: the live view mounts
    renderWithApi(<LiveView load={fakeGame().load} />);
    off();
    // THEN: the scene receives the experiments plus scenario categories for the chaos weather
    expect(seen).toHaveBeenCalledWith({ experiments: [experiment()], categories: { "black-friday": "traffic" } });
  });

  it("shouldLoadThePhaserLayerLazilyWhenTheDefaultLoaderIsUsed", async () => {
    // GIVEN: the production loader with the render module replaced by a stub
    vi.doMock("../live/render/game", () => ({ startLiveGame: () => ({ destroy() {} }) }));
    // WHEN: the loader is invoked
    const start = await loadRenderer();
    // THEN: it resolves to the game starter
    expect(typeof start).toBe("function");
    vi.doUnmock("../live/render/game");
  });
});
