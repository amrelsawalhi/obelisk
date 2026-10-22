import React, { useEffect, useState } from 'react';
import { Routes, Route, Navigate, useLocation, useNavigate } from 'react-router-dom';
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { CommandMenu } from './components/layout/CommandMenu';
import { IngestActivityDock } from './components/layout/IngestActivityDock';
import { ErrorBoundary } from './components/common/ErrorBoundary';

import { HomeView } from './components/home/HomeView';
import { CatalogView } from './components/catalog/CatalogView';
import { QueryStudio } from './components/query/QueryStudio';
import { DbtStudio } from './components/dbt/DbtStudio';
import { NotebooksView } from './components/notebooks/NotebooksView';
import type { NotebookTarget } from './components/notebooks/NotebooksView';
import { PipelinesView } from './components/pipelines/PipelinesView';
import { DashboardsView } from './components/dashboards/DashboardsView';
import { FileManagerView } from './components/files/FileManagerView';
import { TerminalView } from './components/terminal/TerminalView';
import { ServicesView } from './components/services/ServicesView';
import { SettingsView } from './components/settings/SettingsView';

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
  <Routes>
    <Route path="/query/popout" element={<QueryStudio isPopout />} />
    <Route path="*" element={<AppLayout />} />
  </Routes>
);
