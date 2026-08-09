// Framework-agnostic bearer-token manager. Deliberately contains NO React
// imports so it can be unit-tested standalone and used directly by the
// plain-fetch API layer. A React provider bridges it to a password overlay
// via `registerPrompt`.

const STORAGE_KEY = 'fmo_bearer_token'

export interface AuthManager {
  getToken(): string | null
  setToken(token: string): void
  clearToken(): void
  ensureToken(): Promise<string>
  registerPrompt(fn: () => void): void
  hasFailedAttempt(): boolean
}

export function createAuthManager(): AuthManager {
  let inMemoryToken: string | null = null
  let seeded = false
  let pendingPromise: Promise<string> | null = null
  let pendingResolve: ((token: string) => void) | null = null
  let lastAttemptFailed = false
  let onPrompt: (() => void) | null = null

  function seedFromStorage(): void {
    if (seeded) {
      return
    }
    seeded = true
    try {
      inMemoryToken = window.sessionStorage.getItem(STORAGE_KEY)
    } catch {
      inMemoryToken = null
    }
  }

  return {
    getToken(): string | null {
      seedFromStorage()
      return inMemoryToken
    },

    setToken(token: string): void {
      seedFromStorage()
      inMemoryToken = token
      try {
        window.sessionStorage.setItem(STORAGE_KEY, token)
      } catch {
        // Ignore storage failures (private mode, disabled storage); the
        // in-memory token still works for this session.
      }
      lastAttemptFailed = false
      if (pendingResolve) {
        const resolve = pendingResolve
        pendingResolve = null
        resolve(token)
      }
    },

    clearToken(): void {
      seedFromStorage()
      // Distinguish "cold start, never authenticated" (no error to show) from
      // "a token was tried and rejected" (show 'Incorrect password'): we only
      // flag a failed attempt if we actually had a token to clear.
      lastAttemptFailed = inMemoryToken !== null
      inMemoryToken = null
      pendingPromise = null
      pendingResolve = null
      try {
        window.sessionStorage.removeItem(STORAGE_KEY)
      } catch {
        // Ignore storage failures.
      }
    },

    ensureToken(): Promise<string> {
      // Single-flight: concurrent 401s collapse to one prompt.
      if (pendingPromise) {
        return pendingPromise
      }
      pendingPromise = new Promise<string>((resolve) => {
        pendingResolve = resolve
      })
      onPrompt?.()
      return pendingPromise
    },

    registerPrompt(fn: () => void): void {
      onPrompt = fn
    },

    hasFailedAttempt(): boolean {
      return lastAttemptFailed
    },
  }
}

export const auth = createAuthManager()
