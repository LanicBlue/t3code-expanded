import { redirect } from "@tanstack/react-router";

/**
 * Unauthenticated route guard. The bounce to /pair is a FULL navigation, not
 * a client-side route change: gateways in front of the app (e.g. an
 * auto-pairing proxy) intercept real /pair requests and re-mint the session;
 * a client-side redirect would bypass them and strand the user on the static
 * pairing page. The thrown redirect only covers non-browser environments.
 */
export function bounceToPairing(): never {
  if (typeof window !== "undefined") {
    window.location.replace("/pair");
  }
  throw redirect({ to: "/pair", replace: true });
}
