import { useEffect, useMemo, useState } from 'react';
import { api } from '../services/api';
import type { MintDenomination } from '../types/api';
import { asBitcoin } from '../utils/format';
import { EcashDenominationsChart } from './EcashDenominationsChart';

interface EcashTabProps {
  federationId: string;
}

// Ecash denomination histogram tab: how many notes of each power-of-two
// denomination have ever been issued vs. are currently in circulation, plus a
// summary of total note counts and their value.
export function EcashTab({ federationId }: EcashTabProps) {
  const [data, setData] = useState<MintDenomination[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .getMintDenominations(federationId)
      .then((denominations) => {
        if (!cancelled) {
          setData(denominations);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load denominations');
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId]);

  const totals = useMemo(() => {
    if (!data) {
      return null;
    }
    return data.reduce(
      (acc, d) => ({
        issued: acc.issued + d.issued,
        inCirculation: acc.inCirculation + d.in_circulation,
        issuedValueMsat: acc.issuedValueMsat + d.issued * d.denomination_msat,
        circulationValueMsat: acc.circulationValueMsat + d.in_circulation * d.denomination_msat,
      }),
      { issued: 0, inCirculation: 0, issuedValueMsat: 0, circulationValueMsat: 0 }
    );
  }, [data]);

  if (loading) {
    return (
      <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
        Loading denominations…
      </div>
    );
  }

  if (error) {
    return <div className="py-10 text-center text-sm text-red-500">Error: {error}</div>;
  }

  if (!data || data.length === 0) {
    return (
      <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
        No ecash notes observed for this federation
      </div>
    );
  }

  return (
    <div>
      <div className="mb-3 text-xs sm:text-sm text-gray-500 dark:text-gray-400">
        Ecash notes come in fixed power-of-two denominations. Each bar counts the notes of that
        denomination <span className="font-medium text-blue-500">ever issued</span> (left axis) and
        those <span className="font-medium text-emerald-500">currently in circulation</span> (right
        axis, = issued − spent).
      </div>

      {totals && (
        <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
          <SummaryStat label="Notes issued" value={totals.issued.toLocaleString()} />
          <SummaryStat label="In circulation" value={totals.inCirculation.toLocaleString()} />
          <SummaryStat label="Value issued" value={asBitcoin(totals.issuedValueMsat, 8)} />
          <SummaryStat
            label="Value in circulation"
            value={asBitcoin(totals.circulationValueMsat, 8)}
          />
        </div>
      )}

      <EcashDenominationsChart data={data} />
    </div>
  );
}

function SummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/40 px-3 py-2">
      <div className="text-[10px] uppercase tracking-wide text-gray-500 dark:text-gray-400">
        {label}
      </div>
      <div className="text-sm font-semibold text-gray-900 dark:text-white tabular-nums">
        {value}
      </div>
    </div>
  );
}
