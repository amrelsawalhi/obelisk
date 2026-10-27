/**
 * The URL of a deployed service's own web UI.
 *
 * Obelisk is often not running on the machine reading the page: it is on a server on the
 * LAN (or behind a gateway). Hardcoding `localhost` made every "Open UI" link point at the
 * operator's own laptop, where nothing is listening — while the service was fine on the
 * server. The browser already knows which host it used to reach Obelisk, and that is the
 * host to keep.
 *
 * `path` wins when the backend supplied one (a gateway subpath such as `/superset`), since
 * a relative path already resolves against the right origin.
 */
export function serviceUrl(port?: number | string | null, path?: string | null): string {
  if (path) return path;
  const { protocol, hostname } = window.location;
  return `${protocol}//${hostname}${port ? `:${port}` : ''}`;
}
