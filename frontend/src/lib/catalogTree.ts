import type { LakehouseDirectory } from './types';

/**
 * A lakehouse folder plus the folders nested inside it.
 *
 * The catalog API returns folders as a flat list where each row carries
 * `path`, `parent_path` and `depth`. Rendering that list directly made every
 * folder a sibling row, so collapsing a parent only hid its tables and left
 * its subfolders on screen. Rebuilding the hierarchy once keeps the Catalog
 * Tree and the Query Studio schema explorer on the same structure, and lets a
 * collapsed folder unmount its whole subtree.
 */
export interface DirectoryTreeNode extends LakehouseDirectory {
  children: DirectoryTreeNode[];
  /** Tables in this folder and in every folder below it. */
  tableCount: number;
}

function resolveParentPath(
  dir: LakehouseDirectory,
  byPath: Map<string, LakehouseDirectory>
): string | null {
  if (dir.parent_path && byPath.has(dir.parent_path)) return dir.parent_path;

  // Fall back to the longest path prefix when the API omits parent_path
  // (older cached payloads, or hand-written fixtures in tests).
  let best: string | null = null;
  for (const candidate of byPath.values()) {
    if (candidate.path === dir.path) continue;
    if (!dir.path.startsWith(`${candidate.path}/`)) continue;
    if (best === null || candidate.path.length > best.length) best = candidate.path;
  }
  return best;
}

/** Turns the flat `LakehouseDirectory[]` into a sorted folder tree. */
export function buildDirectoryTree(directories: LakehouseDirectory[] = []): DirectoryTreeNode[] {
  const byPath = new Map<string, LakehouseDirectory>();
  directories.forEach((dir) => byPath.set(dir.path, dir));

  const nodesByPath = new Map<string, DirectoryTreeNode>();
  directories.forEach((dir) => {
    nodesByPath.set(dir.path, {
      ...dir,
      tables: dir.tables ?? [],
      children: [],
      tableCount: (dir.tables ?? []).length,
    });
  });

  const roots: DirectoryTreeNode[] = [];
  directories.forEach((dir) => {
    const node = nodesByPath.get(dir.path);
    if (!node) return;
    const parentPath = resolveParentPath(dir, byPath);
    const parent = parentPath ? nodesByPath.get(parentPath) : undefined;
    if (parent) parent.children.push(node);
    else roots.push(node);
  });

  const finalize = (node: DirectoryTreeNode): number => {
    node.children.sort((a, b) => a.name.localeCompare(b.name));
    node.tableCount =
      node.tables.length + node.children.reduce((total, child) => total + finalize(child), 0);
    return node.tableCount;
  };

  roots.sort((a, b) => a.name.localeCompare(b.name));
  roots.forEach(finalize);
  return roots;
}

/** Depth-first walk, parents before children. */
export function flattenDirectoryTree(nodes: DirectoryTreeNode[]): DirectoryTreeNode[] {
  const flat: DirectoryTreeNode[] = [];
  const walk = (list: DirectoryTreeNode[]) => {
    list.forEach((node) => {
      flat.push(node);
      walk(node.children);
    });
  };
  walk(nodes);
  return flat;
}

/**
 * Keeps only the folders that still have a matching table somewhere below
 * them. A folder whose own name matches keeps its entire subtree, so searching
 * for `silver` shows the silver folder as it is rather than an empty shell.
 */
export function filterDirectoryTree(
  nodes: DirectoryTreeNode[],
  query: string
): DirectoryTreeNode[] {
  const needle = query.trim().toLowerCase();
  if (!needle) return nodes;

  const walk = (node: DirectoryTreeNode): DirectoryTreeNode | null => {
    if (node.name.toLowerCase().includes(needle)) return node;

    const children = node.children
      .map(walk)
      .filter((child): child is DirectoryTreeNode => child !== null);
    const tables = node.tables.filter((table) => table.name.toLowerCase().includes(needle));
    if (children.length === 0 && tables.length === 0) return null;

    return {
      ...node,
      children,
      tables,
      tableCount: tables.length + children.reduce((total, child) => total + child.tableCount, 0),
    };
  };

  return nodes
    .map(walk)
    .filter((node): node is DirectoryTreeNode => node !== null);
}
