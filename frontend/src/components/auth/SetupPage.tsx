import React, { useState } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { AlertCircle, CheckCircle2, KeyRound, Loader2, ShieldCheck } from 'lucide-react';
import { useAuth } from '../../context/AuthContext';
import { ObeliskIcon } from '../common/ObeliskIcon';

export const SetupPage: React.FC = () => {
  const { setupAdmin, status, authEnabled } = useAuth();
  const navigate = useNavigate();

  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  // If already authenticated, head straight to the platform
  if (status === 'authenticated') {
    return <Navigate to="/" replace />;
  }

  // If auth is enabled and setup is already complete, go to login
  if (authEnabled && status === 'anonymous') {
    return <Navigate to="/login" replace />;
  }

  const submit = async (event: React.FormEvent) => {
    event.preventDefault();
    setError(null);

    const trimmedUser = username.trim();
    if (!trimmedUser) {
      setError('Username cannot be empty.');
      return;
    }

    if (password.length < 8) {
      setError('Password must be at least 8 characters long.');
      return;
    }

    if (password !== confirmPassword) {
      setError('Passwords do not match.');
      return;
    }

    setPending(true);
    try {
      await setupAdmin(trimmedUser, password);
      navigate('/', { replace: true });
    } catch (err: unknown) {
      const response = (err as { response?: { data?: { detail?: string } } }).response;
      setError(response?.data?.detail ?? 'Failed to complete initial setup. Please try again.');
      setPending(false);
    }
  };

  return (
    <div className="min-h-screen w-full flex items-center justify-center p-6 bg-slate-50">
      <div className="w-full max-w-md flex flex-col">
        {/* Symmetrical Inline Wordmark Header */}
        <div className="flex flex-col mb-7">
          <div className="flex items-center gap-2.5">
            <ObeliskIcon className="w-6 h-6 shrink-0" />
            <span className="text-lg font-black tracking-tight text-slate-950 leading-none">
              Obelisk
            </span>
            <span className="text-slate-300 font-light text-sm">/</span>
            <span className="text-xs font-semibold text-slate-600 tracking-wide">Initial Setup</span>
          </div>
          <p className="text-xs text-slate-500 mt-2 pl-0.5">
            Create the primary administrator account to secure this platform deployment.
          </p>
        </div>

        {/* Clean, Grounded Form Panel */}
        <div className="bg-white border border-slate-200/90 rounded-xl p-6 shadow-2xs space-y-5">
          <form onSubmit={submit} className="space-y-4">
            <div>
              <label
                htmlFor="obelisk-setup-user"
                className="block text-xs font-semibold text-slate-700 mb-1.5"
              >
                Administrator Username
              </label>
              <input
                id="obelisk-setup-user"
                autoComplete="username"
                value={username}
                onChange={(event) => setUsername(event.target.value)}
                className="w-full px-3 py-2 text-sm text-slate-900 bg-white border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-colors font-mono"
                placeholder="admin"
                required
              />
            </div>

            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label
                  htmlFor="obelisk-setup-password"
                  className="block text-xs font-semibold text-slate-700"
                >
                  Master Password
                </label>
                <span className="text-[11px] text-slate-400">Min. 8 characters</span>
              </div>
              <div className="relative">
                <input
                  id="obelisk-setup-password"
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="new-password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                  className="w-full px-3 py-2 pr-12 text-sm text-slate-900 bg-white border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-colors font-mono"
                  placeholder="••••••••"
                  autoFocus
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

            <div>
              <label
                htmlFor="obelisk-setup-confirm"
                className="block text-xs font-semibold text-slate-700 mb-1.5"
              >
                Confirm Master Password
              </label>
              <div className="relative">
                <input
                  id="obelisk-setup-confirm"
                  type={showPassword ? 'text' : 'password'}
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(event) => setConfirmPassword(event.target.value)}
                  className="w-full px-3 py-2 text-sm text-slate-900 bg-white border border-slate-200 rounded-lg focus:outline-none focus:ring-2 focus:ring-sky-500/20 focus:border-sky-500 transition-colors font-mono"
                  placeholder="••••••••"
                  required
                />
                {confirmPassword && password === confirmPassword && (
                  <CheckCircle2 className="w-4 h-4 text-emerald-500 absolute right-3 top-2.5" />
                )}
              </div>
            </div>

            {error && (
              <div className="p-3 bg-rose-50 border border-rose-200/80 rounded-lg text-xs text-rose-700 flex items-start gap-2.5">
                <AlertCircle className="w-4 h-4 text-rose-500 shrink-0 mt-0.5" />
                <span className="leading-relaxed">{error}</span>
              </div>
            )}

            <div className="pt-2">
              <button
                type="submit"
                disabled={pending || !username.trim() || !password || !confirmPassword}
                className="w-full py-2.5 px-4 text-xs font-semibold tracking-wide uppercase text-white bg-slate-900 hover:bg-slate-800 active:bg-slate-950 disabled:opacity-50 disabled:cursor-not-allowed rounded-lg transition-colors flex items-center justify-center gap-2"
              >
                {pending ? (
                  <>
                    <Loader2 className="w-3.5 h-3.5 animate-spin text-slate-300" />
                    <span>Configuring instance…</span>
                  </>
                ) : (
                  <>
                    <KeyRound className="w-3.5 h-3.5" />
                    <span>Create Admin & Launch Platform</span>
                  </>
                )}
              </button>
            </div>
          </form>

          {/* Security footnote */}
          <div className="pt-3 border-t border-slate-100 flex items-start gap-2 text-[11px] text-slate-500">
            <ShieldCheck className="w-4 h-4 text-sky-600 shrink-0 mt-0.5" />
            <span className="leading-normal">
              Stored securely in the platform database with salted scrypt hashing. This account holds full root privileges.
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

