import React, { useEffect, useState } from 'react';
import { Routes, Route, Navigate, useLocation, useNavigate } from 'react-router-dom';
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { CommandMenu } from './components/layout/CommandMenu';
import { IngestActivityDock } from './components/layout/IngestActivityDock';
import { ErrorBoundary } from './components/common/ErrorBoundary';

import { HomeView } from './components/home/HomeView';
import { CatalogView } from './components/catalog/CatalogView';
import type { NotebookTarget } from './components/notebooks/NotebooksView';

// Everything below pulls in something heavy - Monaco, xterm, or a whole studio - so it
// is fetched when the route is actually opened rather than bundled into the first load.
const QueryStudio = React.lazy(() =>
  import('./components/query/QueryStudio').then((m) => ({ default: m.QueryStudio })),
);
const DbtStudio = React.lazy(() =>
  import('./components/dbt/DbtStudio').then((m) => ({ default: m.DbtStudio })),
);
const NotebooksView = React.lazy(() =>
  import('./components/notebooks/NotebooksView').then((m) => ({ default: m.NotebooksView })),
);
const PipelinesView = React.lazy(() =>
  import('./components/pipelines/PipelinesView').then((m) => ({ default: m.PipelinesView })),
);
const DashboardsView = React.lazy(() =>
  import('./components/dashboards/DashboardsView').then((m) => ({ default: m.DashboardsView })),
);
const FileManagerView = React.lazy(() =>
  import('./components/files/FileManagerView').then((m) => ({ default: m.FileManagerView })),
);
const TerminalView = React.lazy(() =>
  import('./components/terminal/TerminalView').then((m) => ({ default: m.TerminalView })),
);
const ServicesView = React.lazy(() =>
  import('./components/services/ServicesView').then((m) => ({ default: m.ServicesView })),
);
const SettingsView = React.lazy(() =>
  import('./components/settings/SettingsView').then((m) => ({ default: m.SettingsView })),
);

import { TAB_PATHS, tabFromPath } from './lib/navigation';
import type { NavigationTab } from './components/layout/Sidebar';

interface RouteState {
  sql?: string;
  target?: NotebookTarget;
}

/** Fills the main content area exactly like the previous per-tab wrapper did. */
const ViewShell: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div className="flex-1 flex min-h-0 overflow-hidden">{children}</div>
);

/** Shown while a lazy route chunk is being fetched. */
const RouteFallback: React.FC<{ label?: string }> = ({ label = 'Loading' }) => (
  <div className="flex-1 flex items-center justify-center bg-white">
    <div className="flex items-center gap-2 text-xs text-slate-500">
      <span className="w-3 h-3 rounded-full border-2 border-slate-300 border-t-sky-500 animate-spin" />
      <span>{label}…</span>
    </div>
  </div>
);

const AppLayout: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const activeTab: NavigationTab = tabFromPath(location.pathname);
  const [isCommandOpen, setIsCommandOpen] = useState(false);

  const navigateToTab = (tab: NavigationTab) => navigate(TAB_PATHS[tab]);

  // Single source of truth for the command palette shortcut (fixes the old
  // double-handler that navigated to Catalog instead of opening the palette).
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setIsCommandOpen((prev) => !prev);
      } else if (e.key === 'Escape') {
        setIsCommandOpen(false);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, []);

  const routeState = (location.state ?? {}) as RouteState;

  return (
    <div className="flex h-screen w-full overflow-hidden bg-white text-slate-900 font-sans antialiased">
      {/* Primary Left Navigation */}
      <Sidebar activeTab={activeTab} onSelectTab={navigateToTab} />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Global Header */}
        <Header activeTab={activeTab} onOpenCommand={() => setIsCommandOpen(true)} />

        {/* Route View */}
        <main className="flex-1 flex min-h-0 overflow-hidden relative">
          <ErrorBoundary key={location.pathname}>
            <React.Suspense fallback={<RouteFallback />}>
              <Routes>
              <Route path="/" element={<ViewShell><HomeView onNavigate={navigateToTab} /></ViewShell>} />
              <Route path="/catalog" element={<ViewShell><CatalogView /></ViewShell>} />
              <Route path="/query" element={<ViewShell><QueryStudio initialSql={routeState.sql} /></ViewShell>} />
              <Route path="/dbt" element={<ViewShell><DbtStudio /></ViewShell>} />
              <Route
                path="/notebooks"
                element={
                  <ViewShell>
                    <NotebooksView
                      target={routeState.target ?? null}
                      onClearTarget={() => navigate('/notebooks', { replace: true })}
                    />
                  </ViewShell>
                }
              />
              <Route
                path="/pipelines"
                element={
                  <ViewShell>
                    <PipelinesView
                      onOpenInQuery={(sql) => navigate('/query', { state: { sql } })}
                      onNavigateToSettings={() => navigate('/settings')}
                    />
                  </ViewShell>
                }
              />
              <Route path="/dashboards" element={<ViewShell><DashboardsView /></ViewShell>} />
              <Route path="/files" element={<ViewShell><FileManagerView /></ViewShell>} />
              <Route path="/terminal" element={<ViewShell><TerminalView /></ViewShell>} />
              <Route path="/services" element={<ViewShell><ServicesView /></ViewShell>} />
              <Route path="/settings" element={<ViewShell><SettingsView /></ViewShell>} />
              <Route path="*" element={<Navigate to="/" replace />} />
              </Routes>
            </React.Suspense>
          </ErrorBoundary>
        </main>
      </div>

      {/* Global Command Palette (Cmd/Ctrl+K) */}
      <CommandMenu isOpen={isCommandOpen} onClose={() => setIsCommandOpen(false)} />

      {/* Global Minimizable Ingestion Activity Dock */}
      <IngestActivityDock />
    </div>
  );
};

export const App: React.FC = () => (
  <React.Suspense fallback={<RouteFallback label="Loading editor" />}>
    <Routes>
      <Route path="/query/popout" element={<QueryStudio isPopout />} />
      <Route path="*" element={<AppLayout />} />
    </Routes>
  </React.Suspense>
);
