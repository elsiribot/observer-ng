import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { GatewaysTab } from './GatewaysTab';
import { api } from '../services/api';
import type { FederationGateways } from '../services/api';
import type { GatewayInfo } from '../types/api';

vi.mock('../services/api', () => ({
  api: {
    getGateways: vi.fn(),
  },
}));

// A rich LNv1 gateway (node key, vetting, activity + uptime metrics).
function makeV1Gateway(overrides: Partial<GatewayInfo> = {}): GatewayInfo {
  return {
    gateway_id: '00112233445566778899aabbccddeeff',
    node_pub_key: 'aabbccddeeff00112233445566778899aabbccddeeff001122334455667788',
    lightning_alias: 'Test Gateway',
    api_endpoint: 'https://gw.example.com/',
    vetted: true,
    first_seen: '2024-01-01T00:00:00Z',
    last_seen: '2024-06-01T00:00:00Z',
    activity_window: {
      fund_count: 12,
      settle_count: 10,
      cancel_count: 2,
      total_volume_msat: 5_000_000,
    },
    uptime_window: {
      sample_count: 100,
      seen_samples: 95,
      online_minutes: 950,
      offline_minutes: 50,
      uptime_pct: 95,
    },
    metrics_window: '7d',
    ...overrides,
  };
}

// A thin LNv2 gateway: the API URL doubles as gateway_id/api_endpoint and the
// rich fields are absent (node_pub_key/vetted default here just to satisfy the
// shared type; the component reads them off `module === 'LNv2'`).
function makeV2Gateway(overrides: Partial<GatewayInfo> = {}): GatewayInfo {
  return {
    gateway_id: 'https://lnv2-gw.example.com/',
    node_pub_key: '',
    lightning_alias: '',
    api_endpoint: 'https://lnv2-gw.example.com/',
    vetted: false,
    first_seen: '2024-02-01T00:00:00Z',
    last_seen: '2024-06-15T00:00:00Z',
    ...overrides,
  };
}

function gateways(partial: Partial<FederationGateways>): FederationGateways {
  return { v1: [], v2: [], ...partial };
}

describe('GatewaysTab', () => {
  beforeEach(() => {
    // clipboard is used by the Copyable component rendered inside the card.
    vi.stubGlobal('navigator', {
      clipboard: { writeText: vi.fn().mockResolvedValue(undefined) },
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('renders an LNv1 gateway with alias, vetted badge, activity and uptime', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce(gateways({ v1: [makeV1Gateway()] }));

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('Test Gateway')).toBeInTheDocument();
    expect(screen.getByText('LNv1')).toBeInTheDocument();
    expect(screen.getByText('Vetted')).toBeInTheDocument();
    // Activity counts.
    expect(screen.getByText('12')).toBeInTheDocument();
    expect(screen.getByText('10')).toBeInTheDocument();
    // Volume rendered in sats (5_000_000 msat = 5000 sats).
    expect(screen.getByText('5,000 sats')).toBeInTheDocument();
    // Uptime percentage.
    expect(screen.getByText(/95\.0%/)).toBeInTheDocument();

    expect(api.getGateways).toHaveBeenCalledWith('fed1', '7d');
  });

  it('renders an LNv2-only federation: URL + tag, no activity/uptime/vetted', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce(gateways({ v2: [makeV2Gateway()] }));

    render(<GatewaysTab federationId="fed1" />);

    // The URL is shown as the endpoint link.
    expect(await screen.findByText('https://lnv2-gw.example.com/')).toBeInTheDocument();
    expect(screen.getByText('LNv2')).toBeInTheDocument();
    // Thin entry: no vetting badge, no activity/uptime, no node-key section.
    expect(screen.queryByText('Vetted')).not.toBeInTheDocument();
    expect(screen.queryByText('Unvetted')).not.toBeInTheDocument();
    expect(screen.queryByText('Uptime')).not.toBeInTheDocument();
    expect(screen.queryByText('Node public key')).not.toBeInTheDocument();
    expect(
      screen.getByText(/No activity or uptime metrics reported for LNv2/)
    ).toBeInTheDocument();
  });

  it('renders a mixed federation: both an LNv1 and an LNv2 gateway', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce(
      gateways({ v1: [makeV1Gateway()], v2: [makeV2Gateway()] })
    );

    render(<GatewaysTab federationId="fed1" />);

    // Both cards present, each with its own module badge.
    expect(await screen.findByText('Test Gateway')).toBeInTheDocument();
    expect(screen.getByText('LNv1')).toBeInTheDocument();
    expect(screen.getByText('LNv2')).toBeInTheDocument();
    expect(screen.getByText('https://lnv2-gw.example.com/')).toBeInTheDocument();
  });

  it('shows the empty state only when BOTH modules are empty', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce(gateways({}));

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('No gateways registered')).toBeInTheDocument();
  });

  it('shows an error state when the fetch fails', async () => {
    vi.mocked(api.getGateways).mockRejectedValueOnce(new Error('boom'));

    render(<GatewaysTab federationId="fed1" />);

    await waitFor(() => expect(screen.getByText(/boom/)).toBeInTheDocument());
  });

  it('handles an LNv1 gateway missing optional metrics without crashing', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce(
      gateways({
        v1: [
          makeV1Gateway({
            activity_window: undefined,
            uptime_window: undefined,
            first_seen: undefined,
            last_seen: undefined,
            vetted: false,
          }),
        ],
      })
    );

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('Test Gateway')).toBeInTheDocument();
    expect(screen.getByText('Unvetted')).toBeInTheDocument();
    expect(screen.getByText('No uptime data')).toBeInTheDocument();
  });
});
