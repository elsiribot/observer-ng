import { describe, it, expect, beforeEach, vi } from 'vitest'
import { createAuthManager } from './auth'

beforeEach(() => {
  window.sessionStorage.clear()
})

describe('auth manager', () => {
  it('returns null when no token is set', () => {
    const auth = createAuthManager()
    expect(auth.getToken()).toBeNull()
  })

  it('persists a token to sessionStorage under fmo_bearer_token', () => {
    const auth = createAuthManager()
    auth.setToken('secret')
    expect(auth.getToken()).toBe('secret')
    expect(window.sessionStorage.getItem('fmo_bearer_token')).toBe('secret')
  })

  it('seeds a fresh manager from an existing sessionStorage token', () => {
    window.sessionStorage.setItem('fmo_bearer_token', 'stored')
    const auth = createAuthManager()
    expect(auth.getToken()).toBe('stored')
  })

  it('clearToken wipes memory and storage', () => {
    const auth = createAuthManager()
    auth.setToken('secret')
    auth.clearToken()
    expect(auth.getToken()).toBeNull()
    expect(window.sessionStorage.getItem('fmo_bearer_token')).toBeNull()
  })

  it('single-flights ensureToken: concurrent calls trigger one prompt and share one result', async () => {
    const auth = createAuthManager()
    const onPrompt = vi.fn() // overlay opens; user submits later, not synchronously
    auth.registerPrompt(onPrompt)

    const p1 = auth.ensureToken()
    const p2 = auth.ensureToken()
    expect(onPrompt).toHaveBeenCalledTimes(1)

    auth.setToken('entered') // user submits the overlay
    expect(await p1).toBe('entered')
    expect(await p2).toBe('entered')
  })

  it('single-flights even when each concurrent request clears the token first (request-wrapper pattern)', async () => {
    const auth = createAuthManager()
    const onPrompt = vi.fn()
    auth.registerPrompt(onPrompt)

    // Two concurrent 401s, each doing what request() does before awaiting a token.
    auth.clearToken()
    const p1 = auth.ensureToken()
    auth.clearToken()
    const p2 = auth.ensureToken()

    expect(onPrompt).toHaveBeenCalledTimes(1)

    auth.setToken('shared')
    expect(await p1).toBe('shared')
    expect(await p2).toBe('shared')
  })

  it('prompts again on a second cycle after a token was set and later cleared', async () => {
    const auth = createAuthManager()
    const onPrompt = vi.fn()
    auth.registerPrompt(onPrompt)

    const p1 = auth.ensureToken()
    auth.setToken('first')
    await p1
    expect(onPrompt).toHaveBeenCalledTimes(1)

    // New 401 cycle.
    auth.clearToken()
    const p2 = auth.ensureToken()
    expect(onPrompt).toHaveBeenCalledTimes(2)
    auth.setToken('second')
    expect(await p2).toBe('second')
  })

  it('hasFailedAttempt is false on cold start and true after a real token is rejected', () => {
    const auth = createAuthManager()
    // Cold start: nothing was ever set, clearing must not flag a failure.
    auth.clearToken()
    expect(auth.hasFailedAttempt()).toBe(false)

    // A token was set (simulating a user entry) and then rejected (cleared).
    auth.setToken('wrong')
    auth.clearToken()
    expect(auth.hasFailedAttempt()).toBe(true)

    // Entering a new token clears the failed flag.
    auth.setToken('right')
    expect(auth.hasFailedAttempt()).toBe(false)
  })
})
