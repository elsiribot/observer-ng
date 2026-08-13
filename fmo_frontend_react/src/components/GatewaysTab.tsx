import { useEffect, useState } from 'react';
import { api } from '../services/api';
import type { FederationGateways, GatewayModule } from '../services/api';
import type { GatewayInfo } from '../types/api';
import { Badge } from './Badge';
import { Alert } from './Alert';
import { Copyable } from './Copyable';
import { asSats, formatNumber, formatTimestamp, timeAgo } from '../utils/format';

interface GatewaysTabProps {
  federationId: string;
}

// The windows the API accepts; we surface a useful subset in the selector.
const WINDOWS = ['24h', '7d', '30d'] as const;
type Window = (typeof WINDOWS)[number];

// The activity counts are contract *lifecycle stages*, not directions: a
// gateway contract is Opened (funded), then either Settled (claimed) or
// Cancelled (refunded). They span both user sends and receives, so "Opened" is
// the first stage of the same lifecycle, not the opposite of the other two.
const ACTIVITY_TOOLTIP =
  'Gateway contracts opened, then settled (claimed) or cancelled (refunded), across both send and receive — lifecycle stages, not directions.';

// `first_seen`/`last_seen` arrive as RFC3339 strings, but the shared
// `formatTimestamp`/`timeAgo` helpers expect unix epoch seconds. Convert here,
// tolerating an unparseable/absent value by returning null (renders "unknown").
function toEpochSeconds(rfc3339: string | undefined): number | null {
  if (!rfc3339) {
    return null;
  }
  const ms = new Date(rfc3339).getTime();
  return Number.isNaN(ms) ? null : Math.floor(ms / 1000);
}

// Shorten a hex id for display (keep head and tail), e.g. "0123…cdef".
function shortenHex(hex: string): string {
  if (hex.length <= 16) {
    return hex;
  }
  return `${hex.slice(0, 8)}…${hex.slice(-8)}`;
}

// A gateway paired with the Lightning module it was registered with, so the
// card can tag it and render the right (rich vs. thin) subset of fields.
interface TaggedGateway {
  module: GatewayModule;
  gateway: GatewayInfo;
}

// Flattens the two per-module lists into a single tagged, ordered list: LNv1
// (richer) first, LNv2 after. The gateway id schemes differ (LNv1 uses a hex
// pubkey, LNv2 the gateway's API URL) so cross-module collisions aren't
// expected; the render key still namespaces by module to be safe.
function tagGateways(data: FederationGateways): TaggedGateway[] {
  return [
    ...data.v1.map((gateway): TaggedGateway => ({ module: 'LNv1', gateway })),
    ...data.v2.map((gateway): TaggedGateway => ({ module: 'LNv2', gateway })),
  ];
}

// Lists a federation's Lightning gateways from both the LNv1 and LNv2 modules.
// LNv1 entries carry rich activity + uptime metrics over a selectable window;
// LNv2 entries are thin (URL + first/last seen). Mirrors the loading/empty/
// error handling of the other detail tabs; the empty state shows only when
// *both* modules return nothing.
export function GatewaysTab({ federationId }: GatewaysTabProps) {
  const [gateways, setGateways] = useState<TaggedGateway[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [window, setWindow] = useState<Window>('7d');

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .getGateways(federationId, window)
      .then((data) => {
        if (!cancelled) {
          setGateways(tagGateways(data));
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : 'Failed to load gateways');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [federationId, window]);

  return (
    <div>
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-4">
        <p className="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
          Lightning gateways registered with this federation, across the LNv1
          and LNv2 modules, with activity and uptime over the selected window
          (LNv1 only).
        </p>
        <div className="relative shrink-0">
          <select
            className="px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-gray-100 dark:bg-gray-700 text-gray-900 dark:text-white text-xs sm:text-sm appearance-none cursor-pointer"
            value={window}
            onChange={(e) => setWindow(e.target.value as Window)}
            aria-label="Metrics window"
          >
            {WINDOWS.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error ? (
        <Alert level="error" message={error} />
      ) : loading ? (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
          Loading gateways…
        </div>
      ) : gateways.length === 0 ? (
        <div className="text-center text-xs sm:text-sm text-gray-500 dark:text-gray-400 py-12">
          No gateways registered
        </div>
      ) : (
        <div className="space-y-4">
          {gateways.map((entry) => (
            <GatewayCard
              key={`${entry.module}:${entry.gateway.gateway_id}`}
              module={entry.module}
              gateway={entry.gateway}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function GatewayCard({ module, gateway }: { module: GatewayModule; gateway: GatewayInfo }) {
  // Only LNv1 carries the rich fields (node key, vetting, activity, uptime).
  // LNv2's registry is thin, so we render just its identity (URL) + seen times
  // and skip the rich sections rather than filling them with placeholders.
  const isV1 = module === 'LNv1';
  const activity = gateway.activity_window;
  const uptime = gateway.uptime_window;
  const firstSeen = toEpochSeconds(gateway.first_seen);
  const lastSeen = toEpochSeconds(gateway.last_seen);
  const nodePubKey = gateway.node_pub_key;

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow-md border border-gray-200 dark:border-gray-700 p-4 sm:p-6">
      <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-2 mb-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <h3 className="text-base sm:text-lg font-semibold text-gray-900 dark:text-white break-words">
              {gateway.lightning_alias || 'Unnamed Gateway'}
            </h3>
            <Badge level="info">{module}</Badge>
            {isV1 && (
              <Badge level={gateway.vetted ? 'success' : 'info'}>
                {gateway.vetted ? 'Vetted' : 'Unvetted'}
              </Badge>
            )}
          </div>
          <a
            href={gateway.api_endpoint}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs sm:text-sm text-blue-600 dark:text-blue-400 hover:underline break-all"
          >
            {gateway.api_endpoint}
          </a>
        </div>
        {(firstSeen !== null || lastSeen !== null) && (
          <div className="text-[10px] sm:text-xs text-gray-500 dark:text-gray-400 sm:text-right shrink-0">
            {lastSeen !== null && (
              <div title={formatTimestamp(lastSeen)}>
                Last seen {timeAgo(lastSeen)}
              </div>
            )}
            {firstSeen !== null && (
              <div title={formatTimestamp(firstSeen)}>
                First seen {timeAgo(firstSeen)}
              </div>
            )}
          </div>
        )}
      </div>

      {nodePubKey && (
        <div className="mb-4">
          <div className="text-[10px] sm:text-xs uppercase text-gray-500 dark:text-gray-400 mb-1">
            Node public key
          </div>
          <div title={nodePubKey}>
            <Copyable text={nodePubKey} />
          </div>
          <div className="text-[10px] sm:text-xs text-gray-400 dark:text-gray-500 font-mono mt-1">
            {shortenHex(nodePubKey)}
          </div>
        </div>
      )}

      {isV1 ? (
        <>
          <div
            className="text-[10px] sm:text-xs uppercase text-gray-500 dark:text-gray-400 mb-1 flex items-center gap-1"
            title={ACTIVITY_TOOLTIP}
          >
            Activity
            <span className="text-gray-400 dark:text-gray-500 normal-case cursor-help">ⓘ</span>
          </div>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 sm:gap-4">
            <Metric
              label="Opened"
              tooltip={`Contracts opened (funded) for this gateway in the window. ${ACTIVITY_TOOLTIP}`}
              value={activity ? formatNumber(activity.fund_count) : '—'}
            />
            <Metric
              label="Settled"
              tooltip={`Opened contracts that were claimed (paid out). ${ACTIVITY_TOOLTIP}`}
              value={activity ? formatNumber(activity.settle_count) : '—'}
            />
            <Metric
              label="Cancelled"
              tooltip={`Opened contracts that were cancelled/refunded. ${ACTIVITY_TOOLTIP}`}
              value={activity ? formatNumber(activity.cancel_count) : '—'}
            />
            <Metric
              label="Volume"
              tooltip="Total msat of the opened (funded) contracts, across both sends and receives."
              value={activity ? asSats(activity.total_volume_msat) : '—'}
            />
          </div>

          <div className="mt-3 pt-3 border-t border-gray-200 dark:border-gray-700">
            <div className="text-[10px] sm:text-xs uppercase text-gray-500 dark:text-gray-400 mb-1">
              Uptime
            </div>
            {uptime ? (
              <div
                className="text-sm sm:text-base text-gray-900 dark:text-white"
                title={
                  `${formatNumber(uptime.seen_samples)}/${formatNumber(uptime.sample_count)} samples seen · ` +
                  `${formatNumber(uptime.online_minutes)} min online · ` +
                  `${formatNumber(uptime.offline_minutes)} min offline`
                }
              >
                {uptime.uptime_pct.toFixed(1)}%
                <span className="ml-2 text-[10px] sm:text-xs text-gray-500 dark:text-gray-400">
                  ({formatNumber(uptime.seen_samples)}/{formatNumber(uptime.sample_count)} samples)
                </span>
              </div>
            ) : (
              <div className="text-sm text-gray-500 dark:text-gray-400">No uptime data</div>
            )}
          </div>
        </>
      ) : (
        // LNv2: the registry exposes only the gateway's API URL and first/last
        // seen; the observer does not (yet) surface reachability/uptime for it.
        <div className="text-xs sm:text-sm text-gray-500 dark:text-gray-400">
          No activity or uptime metrics reported for LNv2 gateways.
        </div>
      )}
    </div>
  );
}

function Metric({ label, value, tooltip }: { label: string; value: string; tooltip?: string }) {
  return (
    <div>
      <div
        className="text-[10px] sm:text-xs uppercase text-gray-500 dark:text-gray-400 mb-0.5"
        title={tooltip}
      >
        {label}
      </div>
      <div className="text-sm sm:text-base font-medium text-gray-900 dark:text-white break-words">
        {value}
      </div>
    </div>
  );
}
