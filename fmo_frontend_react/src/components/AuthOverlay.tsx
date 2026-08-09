import { useState } from 'react';
import { Button } from './Button';

interface AuthOverlayProps {
  failed: boolean;
  onSubmit: (token: string) => void;
}

export function AuthOverlay({ failed, onSubmit }: AuthOverlayProps) {
  const [value, setValue] = useState('');

  const submit = () => {
    const trimmed = value.trim();
    if (trimmed.length === 0) {
      return;
    }
    onSubmit(trimmed);
  };

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      role="dialog"
      aria-modal="true"
    >
      <div className="w-full max-w-sm rounded-lg bg-white p-6 shadow-lg dark:bg-gray-800">
        <h2 className="mb-2 text-lg font-bold text-gray-900 dark:text-white">
          Authentication required
        </h2>
        <p className="mb-4 text-sm text-gray-600 dark:text-gray-400">
          This Fedimint Observer instance is password-protected. Enter the access token to continue.
        </p>
        {failed && (
          <p className="mb-3 text-sm text-red-600 dark:text-red-400">
            Incorrect password. Please try again.
          </p>
        )}
        <form
          onSubmit={(e) => {
            e.preventDefault();
            submit();
          }}
        >
          <input
            type="password"
            autoFocus
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder="Access token"
            aria-label="Access token"
            className="mb-4 w-full rounded-lg border border-gray-300 bg-gray-50 p-2.5 text-sm text-gray-900 focus:border-blue-500 focus:ring-blue-500 dark:border-gray-600 dark:bg-gray-700 dark:text-white"
          />
          <Button colorScheme="primary" className="w-full" onClick={submit}>
            Unlock
          </Button>
        </form>
      </div>
    </div>
  );
}
