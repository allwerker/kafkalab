import { screen } from "@testing-library/react";
import { link } from "../test/fixtures";
import { fakeApi } from "../test/fakeApi";
import { renderWithApi, seedBoard } from "../test/render";
import { Launchpad } from "./Launchpad";

describe("Launchpad", () => {
  beforeEach(() => seedBoard());

  it("shouldOpenEveryToolInANewTabWhenLinksAreLoaded", async () => {
    // GIVEN: the backend lists Grafana (custom URL) and a degraded traces UI with an unknown icon
    const api = fakeApi({
      links: vi.fn().mockResolvedValue([link({ url: "http://grafana.example:3000" }), link({ id: "traces", title: "VictoriaTraces", url: "http://traces.localhost:8080", icon: "unknown-icon", health: "degraded" })]),
    });
    // WHEN: the launchpad renders
    renderWithApi(<Launchpad />, api);
    // THEN: each tool is a card with its health and opens in a new tab
    const card = await screen.findByTestId("link-traces");
    expect(card).toHaveAttribute("target", "_blank");
    expect(screen.getByLabelText("VictoriaTraces degraded")).toBeInTheDocument();
    // AND: dashboard deep links use the Grafana URL from the links
    expect(screen.getByRole("link", { name: /Consumer Lag/ })).toHaveAttribute("href", "http://grafana.example:3000/d/kafka-lag");
  });

  it("shouldFallBackToStateLinksWhenTheLinksRequestFails", async () => {
    // GIVEN: /api/links fails but the state message carried the links
    seedBoard({ links: [link()] });
    const api = fakeApi({ links: vi.fn().mockRejectedValue(new Error("x")) });
    // WHEN: the launchpad renders
    renderWithApi(<Launchpad />, api);
    // THEN: the Grafana card from the state is shown and dashboards use the default base
    expect(await screen.findByTestId("link-grafana")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Lab Home/ })).toHaveAttribute("href", "http://grafana.localhost:8080/d/lab-home");
  });

  it("shouldSayNoLinksWhenNothingIsKnown", () => {
    // GIVEN: neither the state nor the links endpoint know any tools yet
    // WHEN: the launchpad renders
    renderWithApi(<Launchpad />);
    // THEN: an empty hint is shown, dashboards are still listed with default Grafana
    expect(screen.getByText("No links yet.")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Brokers/ })).toHaveAttribute("href", "http://grafana.localhost:8080/d/kafka-broker");
  });
});
