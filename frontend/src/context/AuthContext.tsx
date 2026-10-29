import React, { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { api } from '../lib/api';
import { onLoggedOut } from '../lib/authEvents';

export interface AuthUser {
  username: string;
  role: string;
}

export type AuthStatus = 'loading' | 'needs_setup' | 'anonymous' | 'authenticated';

interface AuthState {
  status: AuthStatus;
  user: AuthUser | null;
  authEnabled: boolean;
}

interface AuthContextValue extends AuthState {
  login: (username: string, password: string) => Promise<void>;
  setupAdmin: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  markLoggedOut: () => void;
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [state, setState] = useState<AuthState>({
    status: 'loading',
    user: null,
    authEnabled: false,
  });

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const status = await api.getAuthStatus();
        if (cancelled) return;
        if (status.needs_setup) {
          setState({
            status: 'needs_setup',
            user: null,
            authEnabled: true,
          });
          return;
        }
        if (!status.auth_enabled) {
          // A single-operator install: everyone is the local admin, no login wall.
          setState({
            status: 'authenticated',
            user: { username: 'local', role: 'admin' },
            authEnabled: false,
          });
          return;
        }
        const me = await api.getAuthMe();
        if (cancelled) return;
        setState({
          status: me.authenticated ? 'authenticated' : 'anonymous',
          user: me.authenticated ? { username: me.username, role: me.role } : null,
          authEnabled: true,
        });
      } catch {
        if (!cancelled) {
          setState({ status: 'anonymous', user: null, authEnabled: true });
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    // The 401 interceptor fires this; the gate then redirects, preserving `next`.
    return onLoggedOut(() => {
      setState((previous) =>
        previous.authEnabled
          ? { ...previous, status: 'anonymous', user: null }
          : previous,
      );
    });
  }, []);

  const value = useMemo<AuthContextValue>(
    () => ({
      ...state,
      async login(username: string, password: string) {
        const session = await api.login(username, password);
        setState({
          status: 'authenticated',
          user: { username: session.user.username, role: session.user.role },
          authEnabled: true,
        });
      },
      async setupAdmin(username: string, password: string) {
        const session = await api.setupAdmin(username, password);
        setState({
          status: 'authenticated',
          user: { username: session.user.username, role: session.user.role },
          authEnabled: true,
        });
      },
      async logout() {
        try {
          await api.logout();
        } finally {
          setState((previous) => ({ ...previous, status: 'anonymous', user: null }));
        }
      },
      markLoggedOut() {
        setState((previous) => ({ ...previous, status: 'anonymous', user: null }));
      },
    }),
    [state],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used inside <AuthProvider>.');
  }
  return context;
}
