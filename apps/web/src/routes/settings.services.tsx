import { createFileRoute } from "@tanstack/react-router";

import { ServicesSettingsPanel } from "../components/settings/ServicesSettings";

export const Route = createFileRoute("/settings/services")({
  component: ServicesSettingsPanel,
});
