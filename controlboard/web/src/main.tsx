import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "@fontsource-variable/inter";
import "@fontsource/jetbrains-mono/400.css";
import "@fontsource/jetbrains-mono/600.css";
import "./styles/theme.css";
import "./styles/app.css";
import { api } from "./api/client";
import { wsUrl } from "./api/socket";
import { App } from "./components/App";
import { EventBus } from "./live/EventBus";
import { connectBoard } from "./store/connection";
import { boardStore } from "./store/store";

connectBoard({ store: boardStore, api, bus: EventBus, url: wsUrl(window.location) });

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
