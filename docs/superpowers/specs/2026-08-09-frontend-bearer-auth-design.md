# Frontend Bearer Auth Design

**Date:** 2026-08-09
**Status:** Approved, ready for planning
**Branch:** modularization (no PR — local branch only, per standing constraint)

## Goal

Let the existing React frontend (`fmo_frontend_react/`) talk to a bearer-token-protected
Fedimint Observer instance. When a request comes back `401`, the app opens a password
overlay, and once the user enters the token it transparently retries the request. On an
unauthenticated instance (the public `observer.fedimint.org`) nothing ever 401s, so the
feature is completely invisible and behavior is unchanged.

The immediate driver is the private instance at `fmo-priv.sirion.io`, which currently has
**no UI** — nginx proxies everything to the backend behind a single bearer-token gate. This
change lets the standard dashboard run against it.

## Context / Current State

- **All API calls funnel through one file**, `src/services/api.ts`: an `api` object whose 9
  methods each do a plain `fetch(`${BASE_URL}${path}`)`, check `response.ok`, and return
  `response.json()`. There is no axios/swr/react-query and no existing fetch interceptor.
- `BASE_URL = import.meta.env.VITE_FMO_API_BASE_URL || 'https://observer.fedimint.org/api'`,
  baked at build time.
- The app root is `src/App.tsx`: `react-router-dom` `<Router>` wrapping `<NavBar>` + `<Routes>`.
  Styling is Tailwind with dark-mode variants; existing primitives include
  `src/components/Alert.tsx` and `src/components/Button.tsx`.
- **No test tooling exists** — `package.json` scripts are only `dev`/`build`/`lint`/`preview`
  (vite + eslint). Vitest must be added for unit tests.
- **The nix frontend build is already parameterized by API base**
  (`fmo_frontend_react = api: buildNpmPackage { … VITE_FMO_API_BASE_URL = api; }`,
  `fmo_frontend_react_default = fmo_frontend_react "http://localhost:3000/api"`). A same-origin
  private build is `fmo_frontend_react "/api"` — no frontend code change needed for the base URL.

### How the private instance is protected (important)

The private vhost (`elsirion-infa/hosts/runner.nix`) enforces a **single bearer token** at the
nginx layer via an agenix-provided `include` (an `if ($http_authorization != "Bearer …")`
style gate). That token is **the same value as `FO_ADMIN_AUTH`**: reads are gated by nginx,
and write endpoints re-check the same token against `FO_ADMIN_AUTH` in the backend. One scheme
end-to-end. nginx returns `401` itself before the request reaches the backend, so the **first**
request on page load (the federations list) already 401s.

Consequence: there is no separate read-only credential. Whoever holds the view token also holds
write/admin. This is acceptable for a personal instance and is **not** changed here — only named.

## Decisions (locked)

1. **Hosting:** co-host the frontend on the private vhost, same-origin. The static shell is
   served unauthenticated at `/`; only `/api/` is gated. No CORS.
2. **Token storage:** `sessionStorage` — survives refresh and in-tab navigation, cleared when the
   tab closes. Re-enter once per browser session.
3. **Trigger:** purely reactive — the overlay only appears in response to an actual `401`.
4. **Retry:** loop, not literally once — a wrong password re-prompts (with an error) rather than
   hard-failing.

## Architecture

Four units, cleanly separated so the logic is testable without React and without a browser:

```
 fetch (api.ts request wrapper) ──401──▶ AuthManager.ensureToken() ──▶ onPrompt callback
        ▲                                        │                          │
        └────────── retry with token ◀───────────┘◀── user submits ─── AuthOverlay (React)
```

### Unit 1 — Auth manager: `src/services/auth.ts` (new, plain TypeScript, no React)

A module singleton. No React imports so it is unit-testable in isolation.

State:
- in-memory `token: string | null`, lazily seeded from `sessionStorage` on first `getToken()`.
- `pendingPrompt: Promise<string> | null` — the single-flight guard.
- `lastAttemptFailed: boolean` — so the overlay can show "Incorrect password".
- `onPrompt: (() => void) | null` — registered by the React provider.

API (exact signatures the other units consume):
- `getToken(): string | null` — returns in-memory token, seeding it from
  `sessionStorage.getItem(STORAGE_KEY)` on first call.
- `setToken(token: string): void` — sets in-memory and `sessionStorage.setItem`; clears
  `lastAttemptFailed`; resolves and clears any `pendingPrompt`.
- `clearToken(): void` — nulls in-memory and `sessionStorage.removeItem`.
- `ensureToken(): Promise<string>` — **single-flight**: if `pendingPrompt` is non-null, return
  it; otherwise create a new promise, stash its `resolve`, set `lastAttemptFailed` appropriately,
  invoke `onPrompt?.()`, and return the promise. Resolved later by `setToken`.
- `registerPrompt(fn: () => void): void` — store the React callback.
- `hasFailedAttempt(): boolean` — read `lastAttemptFailed` for the overlay's error text.

`STORAGE_KEY = 'fmo_bearer_token'`.

Single-flight guarantee: N concurrent 401s all call `ensureToken()`; the first creates the
promise and fires `onPrompt`, the rest receive the same promise. One overlay, one submit, all
resolve together.

### Unit 2 — Request wrapper: `src/services/api.ts` (edit)

Introduce one private helper and route all 9 methods through it:

```ts
import { auth } from './auth';

function withAuth(opts: RequestInit, token: string | null): RequestInit {
  if (!token) return opts;
  return { ...opts, headers: { ...(opts.headers ?? {}), Authorization: `Bearer ${token}` } };
}

async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  let res = await fetch(`${BASE_URL}${path}`, withAuth(opts, auth.getToken()));
  while (res.status === 401) {
    auth.clearToken();                 // stale or wrong
    const token = await auth.ensureToken();  // single-flight overlay; awaits user submit
    res = await fetch(`${BASE_URL}${path}`, withAuth(opts, token));
  }
  if (!res.ok) {
    throw new Error(`Request to ${path} failed: ${res.status}`);
  }
  return res.json() as Promise<T>;
}
```

Each method becomes a one-liner, e.g.
`getTotals: () => request<FedimintTotals>('/federations/totals')`. The existing
per-method error messages are preserved by keeping the method's own `throw` semantics where they
matter, or by accepting the generic message (the plan decides per method; behavior on the public
instance is unchanged because the 401 loop is never entered).

`getFederation` keeps its current "fetch the list and find it" logic, just calling `request`
instead of raw `fetch`.

### Unit 3 — Overlay + provider: `src/components/AuthOverlay.tsx` + `src/services/AuthProvider.tsx` (new); `src/App.tsx` (edit)

- `AuthProvider` mounts at the app root (wrapping the existing `<Router>` subtree in `App.tsx`).
  On mount it calls `auth.registerPrompt(() => setOpen(true))`. It renders `<AuthOverlay>` when
  `open`.
- `AuthOverlay`: a Tailwind modal (dark-mode aware, styled consistently with `Alert`/`Button`)
  containing a single `<input type="password">` and a submit button. On submit:
  `auth.setToken(value)` (which resolves the pending promise and persists to sessionStorage),
  then `setOpen(false)`. If `auth.hasFailedAttempt()` is true, show an "Incorrect password"
  message. A subsequent 401 fires `onPrompt` again → `setOpen(true)` → overlay reopens with the
  error.
- Reactive only: on the public instance `onPrompt` is never called, so `open` stays false and the
  overlay never renders.

### Unit 4 — Infra: `elsirion-infa/hosts/runner.nix` (edit)

Restructure the private vhost so the static shell is public and only `/api/` is gated:

```nix
virtualHosts."${observerDomain}" = {
  enableACME = true;
  forceSSL = true;
  root = <the fmo_frontend_react "/api" build>;   # same-origin API base
  locations."/" = {
    tryFiles = "$uri $uri/ /index.html";          # SPA fallback, unauthenticated
  };
  locations."/api/" = {
    proxyPass = "http://127.0.0.1:5000/";
    extraConfig = ''
      include ${config.age.secrets.fmo-nginx-auth.path};
    '';
  };
};
```

The React build's `VITE_FMO_API_BASE_URL` is `/api` (relative, same-origin), so calls go to
`fmo-priv.sirion.io/api/...` — the gated path on the same host. The bearer gate moves from `/`
to `/api/`. The `fedimint-observer-modular` flake already exposes the parameterized frontend
builder, so this references `fmo_frontend_react "/api"`; the exact wiring (adding the package to
the flake's outputs if not already exposed, and referencing it from the infra flake input) is a
plan detail.

## Data Flow (private instance, cold load)

1. Browser loads `/` (static, unauthenticated) → app boots.
2. `Home` calls `api.getFederations()` → `request('/federations')` → `fetch` with no token →
   nginx returns `401`.
3. `request` clears token, calls `ensureToken()` → overlay opens.
4. User types token, submits → `setToken` persists to sessionStorage and resolves the promise.
5. `request` retries with `Authorization: Bearer <token>` → `200` → data renders.
6. Later calls read the token from the manager (already in memory) and succeed directly. A page
   refresh restores it from sessionStorage — no re-prompt. Wrong token → `401` again → overlay
   reopens with "Incorrect password".

## Error Handling

- **Wrong password:** retry 401 → `clearToken` → `ensureToken` reopens overlay; `lastAttemptFailed`
  drives an "Incorrect password" message. Loop continues until a request succeeds or the user
  navigates away.
- **Genuine backend/network error (non-401):** `res.ok` is false but status ≠ 401 → the loop is
  not entered → the method throws its normal error, surfaced by existing UI error states. No
  overlay.
- **Concurrent 401s:** single-flight collapses them to one overlay (Unit 1 guarantee).

## Testing

Add **vitest** + **jsdom** (dev deps) and a `test` script. Cover the units with real logic:

`src/services/auth.test.ts`:
- single-flight: two concurrent `ensureToken()` calls → `onPrompt` fires exactly once; both
  resolve with the token passed to one `setToken`.
- `setToken` persists to sessionStorage; a fresh manager instance restores it via `getToken`.
- `clearToken` wipes memory and sessionStorage.
- `hasFailedAttempt` reflects a cleared-then-re-prompted cycle.

`src/services/api.test.ts` (mocked `fetch`):
- 401 then 200 → the retry carries `Authorization: Bearer <token>`; the overlay prompt fires once.
- non-401 error → method throws, no prompt.
- happy path with a preloaded token → `Authorization` header present on the first call, no prompt.

`src/components/AuthOverlay.test.tsx` (testing-library): renders on prompt, submit calls
`setToken`, shows the error when `hasFailedAttempt`.

Manual e2e against `fmo-priv.sirion.io`: cold load → prompt → enter token → dashboard loads;
refresh → no re-prompt; enter wrong token → error; public instance → no overlay ever.

## Scope / Non-Goals

- **In scope:** the four units above, vitest setup, the nginx restructure, and referencing the
  `/api` same-origin frontend build for the private host.
- **Out of scope:** a separate read-only credential (view token == admin token, unchanged); a
  logout/"forget token" button (sessionStorage clears on tab close — can be a later addition);
  any change to the public deployment; CORS (not needed given same-origin co-hosting).

## Global Constraints

- No behavior change on the public/unauthenticated instance — the 401 loop must never be entered
  when the server does not 401.
- The auth manager (`auth.ts`) contains no React imports and is unit-testable standalone.
- Token stored only in `sessionStorage` (never `localStorage`), under key `fmo_bearer_token`.
- Same-origin only; do not add CORS handling to the frontend or nginx.
- Work stays on the `modularization` branch; no PR, nothing pushed.
