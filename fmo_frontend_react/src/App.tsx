import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { NavBar } from './components/NavBar';
import { Home } from './pages/Home';
import { Nostr } from './pages/Nostr';
import { FederationDetail } from './pages/FederationDetail';
import { SessionDetail } from './pages/SessionDetail';
import { TransactionDetail } from './pages/TransactionDetail';
import { UserTransaction } from './pages/UserTransaction';
import { AccountDetail } from './pages/AccountDetail';
import { useTheme } from './hooks/useTheme';

function App() {
  const { theme, toggleTheme } = useTheme();

  return (
    <Router>
      <main className="container mx-auto max-w-6xl px-4 min-h-screen pb-4">
        <NavBar theme={theme} onToggleTheme={toggleTheme} />
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/nostr" element={<Nostr />} />
          <Route path="/federations/:id" element={<FederationDetail />} />
          <Route path="/federations/:id/session/:session_index" element={<SessionDetail />} />
          <Route path="/federations/:id/tx/:txid" element={<TransactionDetail />} />
          <Route path="/federations/:id/user-transactions/:key" element={<UserTransaction />} />
          <Route path="/federations/:id/accounts/:account_id" element={<AccountDetail />} />
          <Route path="*" element={<div className="p-4 text-gray-900 dark:text-white">Page not found</div>} />
        </Routes>
      </main>
    </Router>
  );
}

export default App;
