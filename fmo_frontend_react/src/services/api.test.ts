import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest'
import { api, parseSseFrame, subscribeLive } from './api'
import { auth } from './auth'
import type { SessionItem } from '../types/api'

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

describe('parseSseFrame', () => {
  it('parses a bare data frame with a default "message" event', () => {
    const frame = 'data: {"session_index":3,"item_index":1}'

    expect(parseSseFrame(frame)).toEqual({
      event: 'message',
      data: '{"session_index":3,"item_index":1}',
    })
  })

  it('parses an explicit event line', () => {
    const frame = 'event: rollover\ndata: 5'

    expect(parseSseFrame(frame)).toEqual({ event: 'rollover', data: '5' })
  })

  it('concatenates multiple data: lines with a newline, per the SSE spec', () => {
    const frame = 'event: message\ndata: line one\ndata: line two'

    expect(parseSseFrame(frame)).toEqual({
      event: 'message',
      data: 'line one\nline two',
    })
  })

  it('strips a single leading space after "data:" but preserves the rest', () => {
    const frame = 'data:  extra space kept'

    expect(parseSseFrame(frame)).toEqual({ event: 'message', data: ' extra space kept' })
  })
})

// Streams UTF-8-encoded string chunks through a ReadableStream, one per
// `pull`, so tests can simulate `res.body` without a real network request.
function streamOf(chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  let i = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (i < chunks.length) {
        controller.enqueue(encoder.encode(chunks[i]))
        i += 1
      } else {
        controller.close()
      }
    },
  })
}

async function waitFor(check: () => boolean, timeoutMs = 1000): Promise<void> {
  const start = Date.now()
  while (!check()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor: timed out')
    }
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

describe('subscribeLive', () => {
  it('reassembles a frame split across stream chunks and skips SSE keep-alive comment frames', async () => {
    // axum's `Sse::keep_alive` periodically emits a comment-only frame (no
    // `event:`/`data:` field) to hold proxies open; the frame under test here
    // (`data: {json}`) is itself split mid-line across two chunks, exercising
    // the byte-buffering in `readLiveStream`.
    const stream = streamOf([
      ': keep-alive\n\n',
      'data: {"session_index":7,"item_ind',
      'ex":2,"item_type":"ci"}\n\n',
    ])
    const fetchMock = vi
      .fn()
      .mockResolvedValue({ ok: true, status: 200, body: stream } as unknown as Response)
    vi.stubGlobal('fetch', fetchMock)

    const controller = new AbortController()
    const onItem = vi.fn((item: SessionItem) => {
      // Stop the subscription as soon as the real item lands so the test
      // doesn't fall through into the reconnect loop.
      expect(item).toEqual({ session_index: 7, item_index: 2, item_type: 'ci' })
      controller.abort()
    })
    const onError = vi.fn()

    subscribeLive('fed1', { onItem, onError }, controller.signal)

    await waitFor(() => onItem.mock.calls.length > 0)

    expect(onItem).toHaveBeenCalledTimes(1)
    expect(onError).not.toHaveBeenCalled()
  })
})
