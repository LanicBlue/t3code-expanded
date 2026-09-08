import { createFileRoute, redirect } from "@tanstack/react-router";

import { bounceToPairing } from "../lib/authGateBounce";

export const Route = createFileRoute("/projects/$projectKey")({
  beforeLoad: async ({ context, params }) => {
    if (
      context.authGateState.status !== "authenticated" &&
      context.authGateState.status !== "hosted-static"
    ) {
      bounceToPairing();
    }
    throw redirect({
      to: "/settings/projects",
      search: { project: params.projectKey, machine: undefined },
      replace: true,
    });
  },
});
