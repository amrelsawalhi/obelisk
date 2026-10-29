import React, { useState } from 'react';
import { Navigate, useLocation, useNavigate } from 'react-router-dom';
import { AlertCircle, Loader2 } from 'lucide-react';
import { useAuth } from '../../context/AuthContext';
import { ObeliskIcon } from '../common/ObeliskIcon';

/**
 * The one way to see the app while authentication is enforced. Successful login
 * returns to the page the browser asked for (the `next` query parameter), and a
 * `next` that is not a local path is ignored.
 */
export const LoginPage: React.FC = () => {
  const { login, authEnabled, status } = useAuth();
  const navigate = useNavigate();
  const location = useLocation();

  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  const nextParam = new URLSearchParams(location.search).get('next');
  const safeNext =
    nextParam && nextParam.startsWith('/') && !nextParam.startsWith('//') && !nextParam.includes('://')
      ? nextParam
      : '/';

  // First boot requires setting up the admin account
  if (status === 'needs_setup') {
    return <Navigate to="/setup" replace />;
  }

  // Auth is off (or already signed in): this page has nothing to ask for.
  if (!authEnabled || status === 'authenticated') {
    return <Navigate to={safeNext} replace />;
  }

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);
    setPending(true);
    try {
      await login(username, password);
      navigate(safeNext, { replace: true });
    } catch (err: unknown) {
      const response = (err as { response?: { data?: { detail?: string } } }).response;
      setError(response?.data?.detail ?? 'Could not sign in. Check the backend is reachable.');
      setPending(false);
    }
  };

  return (
    <div className="min-h-screen w-full flex items-center justify-center p-6 bg-slate-50">
      <div className="w-full max-w-sm flex flex-col">
        {/* Symmetrical Inline Wordmark Header */}
        <div className="flex flex-col mb-7">
          <div className="flex items-center gap-2.5">
            <ObeliskIcon className="w-6 h-6 shrink-0" />
            <span className="text-lg font-black tracking-tight text-slate-950 leading-none">
              Obelisk
            </span>
            <span className="text-slate-300 font-light text-sm">/</span>
            <span className="text-xs font-semibold text-slate-600 tracking-wide">Sign in</span>
          </div>
          <p className="text-xs text-slate-400 mt-2 pl-0.5">
            Enter your instance credentials to continue
          </p>
        </div>

        {/* Clean, Grounded Form Panel */}
        <div className="bg-white border border-slate-200/90 rounded-xl p-6 shadow-2xs">
          <form onSubmit={submit} className="space-y-4">
            <div>
              <label
                htmlFor="obelisk-login-user"
                className="block text-xs font-semibold text-slate-700 mb-1.5"
              >
                Username
              </label>
              <input
                id="obelisk-login-user"
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                className="w-full px-3 py-2 text-sm text-slate-900 bg-white border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-colors font-mono"
                placeholder="admin"
                autoFocus
                required
              />
            </div>

            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label
                  htmlFor="obelisk-login-password"
                  className="block text-xs font-semibold text-slate-700"
                >
                  Password
                </label>
              </div>
              <div className="relative">
                <input
                  id="obelisk-login-password"
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="current-password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  className="w-full px-3 py-2 pr-12 text-sm text-slate-900 bg-white border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-colors font-mono"
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((prev) => !prev)}
                  className="absolute inset-y-0 right-0 pr-3 flex items-center text-xs font-medium text-slate-400 hover:text-slate-600 focus:outline-none select-none"
                >
                  {showPassword ? 'Hide' : 'Show'}
                </button>
              </div>
            </div>

            {error && (
              <div className="p-3 bg-rose-50 border border-rose-200/80 rounded-lg text-xs text-rose-700 flex items-start gap-2.5">
                <AlertCircle className="w-4 h-4 text-rose-500 shrink-0 mt-0.5" />
                <span className="leading-relaxed">{error}</span>
              </div>
            )}

            <button
              type="submit"
              disabled={pending || !username.trim() || !password}
              className="w-full py-2.5 px-4 text-xs font-semibold tracking-wide uppercase text-white bg-slate-900 hover:bg-slate-800 active:bg-slate-950 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg transition-colors flex items-center justify-center gap-2 mt-2"
            >
              {pending ? (
                <>
                  <Loader2 className="w-3.5 h-3.5 animate-spin text-slate-300" />
                  <span>Signing in…</span>
                </>
              ) : (
                <>
                  <span>Sign in</span>
                  <span className="text-slate-500 font-mono text-[11px]">↵</span>
                </>
              )}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
};
