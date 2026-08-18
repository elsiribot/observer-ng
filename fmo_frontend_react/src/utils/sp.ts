import type { BadgeLevel } from '../components/Badge';

// Human labels + badge levels shared by the Stability Pool tab, account pages
// and transaction-detail links.

export function accTypeLabel(t: string): string {
  switch (t) {
    case 'seeker':
      return 'Seeker';
    case 'provider':
      return 'Provider';
    case 'btc_depositor':
      return 'BTC depositor';
    default:
      return t;
  }
}

// Describes an account's signing structure. `n_keys === null` means the account
// has only ever deposited, so its full `Account` (and thus its multisig shape)
// was never on the ledger to observe.
export function multisigLabel(a: {
  is_multisig: boolean;
  threshold: number | null;
  n_keys: number | null;
}): string {
  if (a.n_keys === null) {
    return 'keys not observed';
  }
  if (a.n_keys <= 1) {
    return 'single-sig';
  }
  return `${a.threshold}-of-${a.n_keys} multisig`;
}

export const SP_KIND_LABEL: Record<string, string> = {
  // Folded account_tx kinds.
  deposit_seek: 'Seek deposit',
  deposit_provide: 'Provide deposit',
  deposit_btc: 'BTC deposit',
  withdraw: 'Withdraw',
  transfer_in: 'Transfer in',
  transfer_out: 'Transfer out',
  // Raw silver action/kind names (from /tx/:txid/accounts and legs).
  deposit_to_seek: 'Seek deposit',
  deposit_to_provide: 'Provide deposit',
  deposit_to_btc_balance: 'BTC deposit',
  withdrawal: 'Withdrawal',
  unlock_for_withdrawal: 'Unlock',
  transfer: 'Transfer',
};

export function spKindLabel(kind: string): string {
  return SP_KIND_LABEL[kind] ?? kind;
}

export function spKindLevel(kind: string): BadgeLevel {
  if (kind.startsWith('deposit') || kind === 'transfer_in') {
    return 'success';
  }
  if (kind === 'withdraw' || kind === 'transfer_out') {
    return 'warning';
  }
  return 'info';
}
