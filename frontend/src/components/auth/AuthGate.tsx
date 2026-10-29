import React from 'react';
import { Navigate, useLocation } from 'react-router-dom';
import { useAuth } from '../../context/AuthContext';

/**
 * Holds the app shell until the session question is answered. With auth on and no
 * session, the visitor is sent to /login with a `next` back to where they were.
 * With auth off (a local install) this renders children immediately.
 */
export const AuthGate: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const { status, authEnabled } = useAuth();
  const location = useLocation();

  if (status === 'loading') {
    return (
      <div className="h-screen flex items-center justify-center bg-white">
        <div className="flex items-center gap-2 text-xs text-slate-500">
          <span className="w-3 h-3 rounded-full border-2 border-slate-300 border-t-sky-500 animate-spin" />
          <span>Checking session…</span>
        </div>
      </div>
    );
  }

  if (authEnabled && status === 'needs_setup') {
    return <Navigate to="/setup" replace />;
  }

  if (authEnabled && status === 'anonymous') {
    const next = encodeURIComponent(location.pathname + location.search);
    return <Navigate to={`/login?next=${next}`} replace />;
  }

  return <>{children}</>;
};
