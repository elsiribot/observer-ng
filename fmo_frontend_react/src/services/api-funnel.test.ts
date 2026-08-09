import { describe, it, expect } from 'vitest'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'

// Invariant: only src/services/api.ts may reference the API base URL. Every other
// module must go through authedFetch/request so bearer auth + the 401 retry apply
// uniformly. This guards against the private-instance regression where pages fetched
// the API directly and bypassed auth.
function walk(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const p = join(dir, entry.name)
    if (entry.isDirectory()) return walk(p)
    return /\.(ts|tsx)$/.test(entry.name) ? [p] : []
  })
}

describe('API funnel invariant', () => {
  it('only services/api.ts references the API base URL directly', () => {
    const allowed = new Set([
      join('src', 'services', 'api.ts'),
      // This guard test itself must reference the env var name to check for it.
      join('src', 'services', 'api-funnel.test.ts'),
    ])
    const offenders = walk('src')
      .filter((file) => !allowed.has(file))
      .filter((file) => readFileSync(file, 'utf8').includes('VITE_FMO_API_BASE_URL'))
    expect(offenders).toEqual([])
  })
})
