import { createFileRoute } from "@tanstack/react-router";

import { ProjectSettingsPage } from "../components/settings/ProjectSettingsPanel";
import { bounceToPairing } from "../lib/authGateBounce";

export const Route = createFileRoute("/projects/$projectKey")({
  beforeLoad: async ({ context }) => {
    if (
      context.authGateState.status !== "authenticated" &&
      context.authGateState.status !== "hosted-static"
    ) {
      bounceToPairing();
    }
  },
  component: () => <ProjectSettingsPage projectKey={Route.useParams().projectKey} />,
});
