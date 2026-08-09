import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { api } from './api'
import { auth } from './auth'

function jsonResponse(body: unknown): Response {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}
function status(code: number): Response {
  return { ok: false, status: code, json: async () => ({}) } as unknown as Response
}

beforeEach(() => {
  window.sessionStorage.clear()
  auth.clearToken()
  // clearToken above sets a benign failed flag if a token lingered; reset by
  // registering a no-op prompt for tests that don't exercise the overlay.
  auth.registerPrompt(() => {})
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('api request wrapper', () => {
  it('attaches Authorization when a token is already set and does not prompt', async () => {
    auth.setToken('tok')
    const onPrompt = vi.fn()
    auth.registerPrompt(onPrompt)
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse([{ id: 'a' }]))
    vi.stubGlobal('fetch', fetchMock)

    const result = await api.getFederations()

    expect(result).toEqual([{ id: 'a' }])
    expect(onPrompt).not.toHaveBeenCalled()
    const [, init] = fetchMock.mock.calls[0]
    expect((init.headers as Record<string, string>).Authorization).toBe('Bearer tok')
  })

  it('on 401 prompts once, then retries with the entered token', async () => {
    const onPrompt = vi.fn(() => auth.setToken('entered'))
    auth.registerPrompt(onPrompt)
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(status(401))
      .mockResolvedValueOnce(jsonResponse([{ id: 'b' }]))
    vi.stubGlobal('fetch', fetchMock)

    const result = await api.getFederations()

    expect(result).toEqual([{ id: 'b' }])
    expect(onPrompt).toHaveBeenCalledTimes(1)
    expect(fetchMock).toHaveBeenCalledTimes(2)
    const [, retryInit] = fetchMock.mock.calls[1]
    expect((retryInit.headers as Record<string, string>).Authorization).toBe('Bearer entered')
  })

  it('throws on a non-401 error without prompting', async () => {
    const onPrompt = vi.fn()
    auth.registerPrompt(onPrompt)
    const fetchMock = vi.fn().mockResolvedValue(status(500))
    vi.stubGlobal('fetch', fetchMock)

    await expect(api.getTotals()).rejects.toThrow()
    expect(onPrompt).not.toHaveBeenCalled()
  })
})
