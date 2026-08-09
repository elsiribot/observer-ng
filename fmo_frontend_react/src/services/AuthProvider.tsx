import { useEffect, useState, type ReactNode } from 'react';
import { auth } from './auth';
import { AuthOverlay } from '../components/AuthOverlay';

// Bridges the framework-agnostic auth manager to the password overlay: when a
// request 401s, the manager fires the registered prompt callback, which opens
// the overlay. Submitting stores the token (resolving the pending request) and
// closes the overlay. Purely reactive — on an unauthenticated instance the
// prompt is never fired and nothing renders.
export function AuthProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState(false);

  useEffect(() => {
    auth.registerPrompt(() => setOpen(true));
  }, []);

  const handleSubmit = (token: string) => {
    auth.setToken(token);
    setOpen(false);
  };

  return (
    <>
      {children}
      {open && <AuthOverlay failed={auth.hasFailedAttempt()} onSubmit={handleSubmit} />}
    </>
  );
}
