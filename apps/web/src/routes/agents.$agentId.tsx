import { createFileRoute, redirect } from "@tanstack/react-router";

import { AgentSettingsPage } from "../components/settings/AgentSettingsPanel";

export const Route = createFileRoute("/agents/$agentId")({
  beforeLoad: async ({ context }) => {
    if (
      context.authGateState.status !== "authenticated" &&
      context.authGateState.status !== "hosted-static"
    ) {
      throw redirect({ to: "/pair", replace: true });
    }
  },
  component: () => <AgentSettingsPage agentId={Route.useParams().agentId} />,
});
