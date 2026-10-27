import React, { useEffect, useMemo, useState } from 'react';
import { Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { CommandMenu } from './components/layout/CommandMenu';
import { IngestActivityDock } from './components/layout/IngestActivityDock';
import { ErrorBoundary } from './components/common/ErrorBoundary';
import { ViewActivityProvider } from './lib/viewActivity';

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
import { ToastHost } from './components/common/ToastHost';

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
  /** Service to open on arrival, so /dashboards can hand off to Catalog → Services. */
  service?: string;
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

/**
 * Views that host an iframe: Marimo keeps polling, animating and holding its own
 * render tree behind `visibility: hidden`, so it is the most expensive thing to park -
 * and the first thing to let go of when off screen. Dashboards is not on this list any
 * more: it links to Superset instead of framing it, so it parks like any other view.
 */
const FRAME_HOSTING_TABS: NavigationTab[] = ['notebooks'];

/** Parked views that hold unsaved work, so they are never evicted for a cheaper one. */
const STATEFUL_WORKSPACES: NavigationTab[] = ['query', 'dbt', 'pipelines'];

/** How many views may stay parked behind the active one. */
const MAX_PARKED_VIEWS = 3;

const AppLayout: React.FC = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const activeTab: NavigationTab = tabFromPath(location.pathname);
  const [isCommandOpen, setIsCommandOpen] = useState(false);

  // A view mounts on first visit and then stays parked, so switching tabs is a
  // visibility change instead of a teardown: unsaved SQL, open dbt tabs, the
  // selected catalog object and dock sizes all survive. Parking is not free,
  // though - a mounted studio is RAM the browser never gives back - so the set is
  // bounded: the active view plus the three most recently visited others.
  const [visitOrder, setVisitOrder] = useState<NavigationTab[]>([activeTab]);

  useEffect(() => {
    setVisitOrder((prev) =>
      prev[prev.length - 1] === activeTab ? prev : [...prev.filter((tab) => tab !== activeTab), activeTab],
    );
  }, [activeTab]);

  const mountedTabs = useMemo(() => {
    const candidates = visitOrder.filter(
      (tab) => tab !== activeTab && !FRAME_HOSTING_TABS.includes(tab),
    );
    // Unsaved edits live in the three workspaces, so they claim the slots first;
    // anything else parks in whatever is left, which is why a visited catalog
    // keeps its selection instead of being dropped for an older studio.
    const stateful = candidates.filter((tab) => STATEFUL_WORKSPACES.includes(tab)).slice(-MAX_PARKED_VIEWS);
    const rest = candidates
      .filter((tab) => !STATEFUL_WORKSPACES.includes(tab))
      .slice(-Math.max(0, MAX_PARKED_VIEWS - stateful.length));
    return [...rest, ...stateful, activeTab];
  }, [visitOrder, activeTab]);

  // The old <Route path="*"> sent unknown URLs home; keep that behaviour now
  // that the router is not doing the rendering.
  useEffect(() => {
    const clean = location.pathname.replace(/\/+$/, '') || '/';
    if (!Object.values(TAB_PATHS).includes(clean)) {
      navigate('/', { replace: true });
    }
  }, [location.pathname, navigate]);

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

  const renderView = (tab: NavigationTab): React.ReactNode => {
    switch (tab) {
      case 'home':
        return <HomeView onNavigate={navigateToTab} />;
      case 'catalog':
        return <CatalogView />;
      case 'query':
        return <QueryStudio initialSql={routeState.sql} />;
      case 'dbt':
        return <DbtStudio />;
      case 'notebooks':
        return (
          <NotebooksView
            target={routeState.target ?? null}
            onClearTarget={() => navigate('/notebooks', { replace: true })}
          />
        );
      case 'pipelines':
        return (
          <PipelinesView
            onOpenInQuery={(sql) => navigate('/query', { state: { sql } })}
            onNavigateToSettings={() => navigate('/settings')}
          />
        );
      case 'dashboards':
        return (
          <DashboardsView
            onDeploySuperset={() => navigate('/services', { state: { service: 'superset' } })}
          />
        );
      case 'files':
        return <FileManagerView />;
      case 'terminal':
        return <TerminalView />;
      case 'services':
        return <ServicesView />;
      case 'settings':
        return <SettingsView />;
      default:
        return null;
    }
  };

  return (
    <div className="flex h-screen w-full overflow-hidden bg-white text-slate-900 font-sans antialiased">
      {/* Primary Left Navigation */}
      <Sidebar activeTab={activeTab} onSelectTab={navigateToTab} />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Global Header */}
        <Header
          activeTab={activeTab}
          onOpenCommand={() => setIsCommandOpen(true)}
          onNavigate={navigateToTab}
        />

        {/* Mounted views. Inactive ones stay laid out but invisible: `visibility`
            keeps their real size, so Monaco and xterm come back without a
            relayout, which `display: none` would force. */}
        <main className="flex-1 flex min-h-0 overflow-hidden relative">
          {mountedTabs.map((tab) => {
            const isActive = tab === activeTab;
            return (
              <div
                key={tab}
                aria-hidden={!isActive}
                className={`absolute inset-0 flex min-h-0 overflow-hidden ${
                  isActive ? 'z-10 visible' : 'obelisk-inactive-view pointer-events-none'
                }`}
              >
                <ViewActivityProvider value={isActive}>
                  {/* One boundary per view: a crash in one tab cannot take the
                      others down, and the mounted state of the rest survives. */}
                  <ErrorBoundary>
                    <React.Suspense fallback={<RouteFallback />}>
                      <ViewShell>{renderView(tab)}</ViewShell>
                    </React.Suspense>
                  </ErrorBoundary>
                </ViewActivityProvider>
              </div>
            );
          })}
        </main>
      </div>

      {/* Global Command Palette (Cmd/Ctrl+K) */}
      <CommandMenu isOpen={isCommandOpen} onClose={() => setIsCommandOpen(false)} />

      {/* Global Minimizable Ingestion Activity Dock */}
      <IngestActivityDock />

      {/* One toast stack for the whole app: any screen can raise a notification. */}
      <ToastHost />
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
