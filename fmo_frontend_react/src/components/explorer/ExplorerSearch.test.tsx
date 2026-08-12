import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { classifySearch, ExplorerSearch } from './ExplorerSearch';

describe('classifySearch', () => {
  const fed = 'fed1';

  it('classifies a 64-hex string as a transaction, lowercased', () => {
    const hex64 = 'ABC123'.padEnd(64, '0');
    expect(classifySearch(fed, hex64)).toEqual({
      path: `/federations/${fed}/tx/${hex64.toLowerCase()}`,
    });
  });

  it('classifies all-digits as a session number', () => {
    expect(classifySearch(fed, '42')).toEqual({
      path: `/federations/${fed}/session/42`,
    });
  });

  it('classifies short hex as a user-transaction key, lowercased', () => {
    expect(classifySearch(fed, 'DEADBEEF')).toEqual({
      path: `/federations/${fed}/user-transactions/deadbeef`,
    });
  });

  it('returns an error for empty input', () => {
    expect(classifySearch(fed, '   ')).toEqual({
      error: 'Enter a transaction id, session number, or user-transaction key',
    });
  });

  it('returns an error for unrecognized input', () => {
    expect(classifySearch(fed, 'not-hex!')).toEqual({
      error: 'Unrecognized — expected a hex id or a session number',
    });
  });
});

describe('ExplorerSearch', () => {
  it('navigates to the session route when a session number is submitted', async () => {
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={['/federations/fed1']}>
        <Routes>
          <Route path="/federations/:id" element={<ExplorerSearch federationId="fed1" />} />
          <Route path="/federations/:id/session/:session_index" element={<div>Session Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText('Search txid, session #, or user-tx key');
    await user.type(input, '42');
    await user.keyboard('{Enter}');

    expect(await screen.findByText('Session Page')).toBeInTheDocument();
  });

  it('shows an inline error and does not navigate for junk input', async () => {
    const user = userEvent.setup();
    render(
      <MemoryRouter initialEntries={['/federations/fed1']}>
        <Routes>
          <Route path="/federations/:id" element={<ExplorerSearch federationId="fed1" />} />
          <Route path="/federations/:id/session/:session_index" element={<div>Session Page</div>} />
        </Routes>
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText('Search txid, session #, or user-tx key');
    await user.type(input, 'not-hex!');
    await user.keyboard('{Enter}');

    expect(await screen.findByText(/Unrecognized/)).toBeInTheDocument();
    expect(screen.queryByText('Session Page')).not.toBeInTheDocument();
  });
});
