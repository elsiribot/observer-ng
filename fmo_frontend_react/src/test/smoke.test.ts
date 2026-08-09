import { describe, it, expect } from 'vitest'

describe('test harness', () => {
  it('runs in a jsdom environment with sessionStorage', () => {
    expect(typeof window).toBe('object')
    expect(window.sessionStorage).toBeDefined()
    window.sessionStorage.setItem('k', 'v')
    expect(window.sessionStorage.getItem('k')).toBe('v')
    window.sessionStorage.clear()
  })
})
