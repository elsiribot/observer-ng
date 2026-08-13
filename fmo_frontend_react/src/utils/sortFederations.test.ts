import { describe, it, expect } from 'vitest';
import type { FederationSummary } from '../types/api';
import { ratingIndex } from './format';
import {
  DEFAULT_SORT_KEY,
  compareFederations,
  sortFederations,
  type SortKey,
} from './sortFederations';

// Minimal federation factory; only the fields the comparators read matter.
function fed(
  overrides: Partial<FederationSummary> & { id: string }
): FederationSummary {
  return {
    id: overrides.id,
    name: overrides.name ?? 'Fed',
    last_7d_activity: [],
    deposits: overrides.deposits ?? 0,
    invite: '',
    nostr_votes: overrides.nostr_votes ?? { count: 0, avg: null },
    health: 'online',
    total_volume: overrides.total_volume ?? 0,
    total_tx_count: overrides.total_tx_count ?? 0,
  };
}

const ids = (feds: FederationSummary[]) => feds.map((f) => f.id);

describe('sortFederations', () => {
  it('defaults to reputation, best first', () => {
    expect(DEFAULT_SORT_KEY).toBe('reputation');

    const low = fed({ id: 'low', nostr_votes: { count: 3, avg: 3.0 } });
    const high = fed({ id: 'high', nostr_votes: { count: 50, avg: 4.8 } });
    const none = fed({ id: 'none', nostr_votes: { count: 0, avg: null } });

    const sorted = sortFederations([low, none, high], 'reputation', 'desc');
    expect(ids(sorted)).toEqual(['high', 'low', 'none']);
  });

  it('matches the historical ratingIndex ordering for the default sort', () => {
    const feds = [
      fed({ id: 'a', nostr_votes: { count: 10, avg: 4.0 } }),
      fed({ id: 'b', nostr_votes: { count: 2, avg: 5.0 } }),
      fed({ id: 'c', nostr_votes: { count: 100, avg: 3.5 } }),
      fed({ id: 'd', nostr_votes: { count: 0, avg: null } }),
    ];

    // Reproduce the previous inline home-page sort: ratingIndex descending
    // with `avg || 0` (so null-vote feds fall to index 0 at the bottom).
    const expected = [...feds].sort(
      (x, y) =>
        ratingIndex(y.nostr_votes.count, y.nostr_votes.avg) -
        ratingIndex(x.nostr_votes.count, x.nostr_votes.avg)
    );

    const sorted = sortFederations(feds, DEFAULT_SORT_KEY, 'desc');
    expect(ids(sorted)).toEqual(ids(expected));
  });

  it('puts null-reputation feds last regardless of direction', () => {
    const withVotes = fed({ id: 'voted', nostr_votes: { count: 5, avg: 4.0 } });
    const noVotes = fed({ id: 'unvoted', nostr_votes: { count: 0, avg: null } });

    expect(ids(sortFederations([noVotes, withVotes], 'reputation', 'desc'))).toEqual([
      'voted',
      'unvoted',
    ]);
    // Ascending must STILL keep the no-vote fed last.
    expect(ids(sortFederations([withVotes, noVotes], 'reputation', 'asc'))).toEqual([
      'voted',
      'unvoted',
    ]);
  });

  it('sorts numeric metrics descending (biggest first)', () => {
    const cases: { key: SortKey; field: keyof FederationSummary }[] = [
      { key: 'volume', field: 'total_volume' },
      { key: 'balance', field: 'deposits' },
      { key: 'tx_count', field: 'total_tx_count' },
    ];

    for (const { key, field } of cases) {
      const small = fed({ id: 'small', [field]: 10 } as never);
      const big = fed({ id: 'big', [field]: 1000 } as never);
      const mid = fed({ id: 'mid', [field]: 100 } as never);

      expect(ids(sortFederations([small, big, mid], key, 'desc'))).toEqual([
        'big',
        'mid',
        'small',
      ]);
      // Direction toggle flips it.
      expect(ids(sortFederations([small, big, mid], key, 'asc'))).toEqual([
        'small',
        'mid',
        'big',
      ]);
    }
  });

  it('sorts by name ascending, case-insensitively', () => {
    const feds = [
      fed({ id: 'z', name: 'Zebra' }),
      fed({ id: 'a', name: 'alpha' }),
      fed({ id: 'm', name: 'Mango' }),
    ];
    expect(ids(sortFederations(feds, 'name', 'asc'))).toEqual(['a', 'm', 'z']);
    expect(ids(sortFederations(feds, 'name', 'desc'))).toEqual(['z', 'm', 'a']);
  });

  it('does not mutate the input array', () => {
    const feds = [
      fed({ id: 'a', total_volume: 1 }),
      fed({ id: 'b', total_volume: 2 }),
    ];
    const before = ids(feds);
    sortFederations(feds, 'volume', 'desc');
    expect(ids(feds)).toEqual(before);
  });

  it('compareFederations returns 0 for two null-reputation feds', () => {
    const a = fed({ id: 'a', nostr_votes: { count: 0, avg: null } });
    const b = fed({ id: 'b', nostr_votes: { count: 0, avg: null } });
    expect(compareFederations(a, b, 'reputation', 'desc')).toBe(0);
  });
});
