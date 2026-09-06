import { createFileRoute } from "@tanstack/react-router";

import { ImBridgeSettingsPanel } from "../components/settings/ImBridgeSettings";

function SettingsImRoute() {
  return <ImBridgeSettingsPanel />;
}

export const Route = createFileRoute("/settings/im")({
  component: SettingsImRoute,
});
