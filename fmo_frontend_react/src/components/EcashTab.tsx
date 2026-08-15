import { useEffect, useState } from 'react';
import { api } from '../services/api';
import type { EcashAnonScatter, MintDenomination } from '../types/api';
import { asBitcoin } from '../utils/format';
import { EcashAnonScatterChart } from './EcashAnonScatterChart';
import { EcashDenominationsChart } from './EcashDenominationsChart';

interface EcashTabProps {
  federationId: string;
}

// One mint module's denominations, tagged with a human label so a federation
// running both the legacy `mint` and next-gen `mintv2` modules gets a labeled
// section per module. A module that returned no data is dropped before render.
interface MintModule {
  label: string;
  data: MintDenomination[];
}

// Aggregate note counts/values across all denominations of one module, for the
// per-module summary line.
function summarize(data: MintDenomination[]) {
  return data.reduce(
    (acc, d) => ({
      issued: acc.issued + d.issued,
      inCirculation: acc.inCirculation + d.in_circulation,
      issuedValueMsat: acc.issuedValueMsat + d.issued * d.denomination_msat,
      circulationValueMsat: acc.circulationValueMsat + d.in_circulation * d.denomination_msat,
    }),
    { issued: 0, inCirculation: 0, issuedValueMsat: 0, circulationValueMsat: 0 }
  );
}

// Ecash denomination histogram tab: how many notes of each power-of-two
// denomination have ever been issued vs. are currently in circulation, plus a
// summary of total note counts and their value. Fetches both the `mint` and
// `mintv2` modules and renders a labeled section for each that has data; the
// empty state shows only when *both* return nothing.
export function EcashTab({ federationId }: EcashTabProps) {
  const [modules, setModules] = useState<MintModule[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetched and loaded independently from the denomination histogram above:
  // a slow/failing anon-scatter query shouldn't block the (usually cheap)
  // histogram from rendering.
  const [scatter, setScatter] = useState<EcashAnonScatter | null>(null);
  const [scatterLoading, setScatterLoading] = useState(true);
  const [scatterError, setScatterError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setScatterLoading(true);
    setScatterError(null);
    api
      .getEcashAnonScatter(federationId)
      .then((data) => {
        if (!cancelled) {
          setScatter(data);
          setScatterLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setScatterError(err instanceof Error ? err.message : 'Failed to load anonymity data');
          setScatterLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    // Each module is fetched independently; a failing/absent module (e.g. a 404
    // when the federation doesn't run it) is treated as an empty list rather
    // than failing the whole tab.
    Promise.all([
      api.getMintDenominations(federationId).catch(() => [] as MintDenomination[]),
      api.getMintV2Denominations(federationId).catch(() => [] as MintDenomination[]),
    ])
      .then(([mint, mintV2]) => {
        if (!cancelled) {
          setModules(
            [
              { label: 'Mint', data: mint },
              { label: 'Mint v2', data: mintV2 },
            ].filter((m) => m.data.length > 0)
          );
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

  if (!modules || modules.length === 0) {
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

      <div className="space-y-8">
        {modules.map((module) => (
          <MintModuleSection
            key={module.label}
            // Only show a per-module heading when more than one module has data,
            // so the common single-module case stays uncluttered.
            heading={modules.length > 1 ? module.label : null}
            data={module.data}
          />
        ))}
      </div>

      <div className="mt-8">
        <h3 className="mb-1 text-sm font-semibold text-gray-900 dark:text-white">
          Transaction anonymity over time
        </h3>
        <div className="mb-3 text-xs sm:text-sm text-gray-500 dark:text-gray-400">
          Each dot is an ecash-spending transaction&apos;s upper-bound anonymity set; lines are
          rolling 7-day percentiles.
        </div>

        {scatterLoading && (
          <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
            Loading anonymity data…
          </div>
        )}

        {!scatterLoading && scatterError && (
          <div className="py-10 text-center text-sm text-red-500">Error: {scatterError}</div>
        )}

        {!scatterLoading && !scatterError && (!scatter || scatter.points.length === 0) && (
          <div className="py-10 text-center text-sm text-gray-500 dark:text-gray-400">
            No anonymity data yet
          </div>
        )}

        {!scatterLoading && !scatterError && scatter && scatter.points.length > 0 && (
          <EcashAnonScatterChart data={scatter} />
        )}
      </div>
    </div>
  );
}

function MintModuleSection({ heading, data }: { heading: string | null; data: MintDenomination[] }) {
  const totals = summarize(data);

  return (
    <div>
      {heading && (
        <h3 className="mb-3 text-sm font-semibold text-gray-900 dark:text-white">{heading}</h3>
      )}

      <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <SummaryStat label="Notes issued" value={totals.issued.toLocaleString()} />
        <SummaryStat label="In circulation" value={totals.inCirculation.toLocaleString()} />
        <SummaryStat label="Value issued" value={asBitcoin(totals.issuedValueMsat, 8)} />
        <SummaryStat
          label="Value in circulation"
          value={asBitcoin(totals.circulationValueMsat, 8)}
        />
      </div>

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
