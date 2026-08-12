/* eslint-disable react-refresh/only-export-components --
 * This module intentionally pairs the `ExplorerSearch` component with its
 * pure, independently-unit-tested classifier `classifySearch`; the two are
 * tightly coupled and small enough that a separate file would be overkill. */
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

export type SearchTarget = { path: string } | { error: string };

const ALL_DIGITS = /^\d+$/;
const HEX_64 = /^[0-9a-fA-F]{64}$/;
const HEX_ANY = /^[0-9a-fA-F]+$/;

// Classifies free-form explorer search input into a route within the current
// federation. All-digits jumps to a session; 64-hex is treated as a txid
// (a 64-hex value is also a valid LN user-tx key, but the tx-detail page
// links onward to its user transaction, so favoring the txid here still
// gets you there); other hex jumps straight to a user transaction.
export function classifySearch(federationId: string, raw: string): SearchTarget {
  const trimmed = raw.trim();
  if (trimmed === '') {
    return { error: 'Enter a transaction id, session number, or user-transaction key' };
  }
  if (ALL_DIGITS.test(trimmed)) {
    return { path: `/federations/${federationId}/session/${parseInt(trimmed, 10)}` };
  }
  if (HEX_64.test(trimmed)) {
    return { path: `/federations/${federationId}/tx/${trimmed.toLowerCase()}` };
  }
  if (HEX_ANY.test(trimmed)) {
    return { path: `/federations/${federationId}/user-transactions/${trimmed.toLowerCase()}` };
  }
  return { error: 'Unrecognized — expected a hex id or a session number' };
}

interface ExplorerSearchProps {
  federationId: string;
}

export function ExplorerSearch({ federationId }: ExplorerSearchProps) {
  const [value, setValue] = useState('');
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const target = classifySearch(federationId, value);
    if ('error' in target) {
      setError(target.error);
      return;
    }
    setError(null);
    navigate(target.path);
  };

  return (
    <form onSubmit={handleSubmit} className="w-full sm:max-w-sm">
      <div className="flex gap-2">
        <input
          type="text"
          value={value}
          onChange={(e) => {
            setValue(e.target.value);
            if (error) setError(null);
          }}
          placeholder="Search txid, session #, or user-tx key"
          aria-label="Search txid, session #, or user-tx key"
          className="block w-full px-3 py-2 text-xs sm:text-sm text-gray-900 border border-gray-300 rounded-lg bg-white dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white focus:ring-blue-500 focus:border-blue-500"
        />
        <button
          type="submit"
          className="px-3 py-2 rounded-lg text-xs sm:text-sm font-medium border whitespace-nowrap bg-gray-100 text-gray-700 border-gray-300 hover:bg-gray-200 dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600 dark:hover:bg-gray-700"
        >
          Go
        </button>
      </div>
      {error && (
        <div className="mt-1 text-xs text-red-500/80 dark:text-red-400/80">{error}</div>
      )}
    </form>
  );
}
