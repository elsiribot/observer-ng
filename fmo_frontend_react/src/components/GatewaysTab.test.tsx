import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { GatewaysTab } from './GatewaysTab';
import { api } from '../services/api';
import type { GatewayInfo } from '../types/api';

vi.mock('../services/api', () => ({
  api: {
    getGateways: vi.fn(),
  },
}));

function makeGateway(overrides: Partial<GatewayInfo> = {}): GatewayInfo {
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

  it('renders a gateway with alias, vetted badge, activity and uptime', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce([makeGateway()]);

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('Test Gateway')).toBeInTheDocument();
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

  it('shows an empty state when there are no gateways', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce([]);

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('No gateways registered')).toBeInTheDocument();
  });

  it('shows an error state when the fetch fails', async () => {
    vi.mocked(api.getGateways).mockRejectedValueOnce(new Error('boom'));

    render(<GatewaysTab federationId="fed1" />);

    await waitFor(() => expect(screen.getByText(/boom/)).toBeInTheDocument());
  });

  it('handles a gateway missing optional metrics without crashing', async () => {
    vi.mocked(api.getGateways).mockResolvedValueOnce([
      makeGateway({
        activity_window: undefined,
        uptime_window: undefined,
        first_seen: undefined,
        last_seen: undefined,
        vetted: false,
      }),
    ]);

    render(<GatewaysTab federationId="fed1" />);

    expect(await screen.findByText('Test Gateway')).toBeInTheDocument();
    expect(screen.getByText('Unvetted')).toBeInTheDocument();
    expect(screen.getByText('No uptime data')).toBeInTheDocument();
  });
});
