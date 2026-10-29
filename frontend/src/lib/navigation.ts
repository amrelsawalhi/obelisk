import { useLocation, useNavigate } from 'react-router-dom';
import type { NavigationTab } from '../components/layout/Sidebar';

/**
 * Single source of truth mapping navigation tabs to URL paths.
 * The sidebar, header, command palette, and home view all resolve through here.
 */
export const TAB_PATHS: Record<NavigationTab, string> = {
  home: '/',
  catalog: '/catalog',
  query: '/query',
  dbt: '/dbt',
  scripts: '/scripts',
  notebooks: '/notebooks',
  pipelines: '/pipelines',
  dashboards: '/dashboards',
  terminal: '/terminal',
  services: '/services',
  settings: '/settings',
};

const PATH_TO_TAB = Object.entries(TAB_PATHS) as [NavigationTab, string][];

/** Resolve the active tab from a pathname (defaults to 'home'). */
export function tabFromPath(pathname: string): NavigationTab {
  const clean = pathname === '/' ? '/' : pathname.replace(/\/+$/, '');
  if (clean === '/' || clean === '') return 'home';

  const match = PATH_TO_TAB.find(
    ([, path]) => path !== '/' && (clean === path || clean.startsWith(`${path}/`))
  );
  return match ? match[0] : 'home';
}

/** Hook returning the active tab plus a navigator that accepts tab ids. */
export function useTabNavigation() {
  const location = useLocation();
  const navigate = useNavigate();
  return {
    activeTab: tabFromPath(location.pathname),
    navigateToTab: (tab: NavigationTab) => navigate(TAB_PATHS[tab]),
  };
}
