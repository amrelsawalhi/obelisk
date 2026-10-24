import type { EdgePayload, PipelineStepDef } from './types';

/** Geometry of a laid-out node, matching `StepNode`'s width and rough height. */
const NODE_WIDTH = 226;
const NODE_HEIGHT = 96;
const GAP_X = 96;
const GAP_Y = 64;

export interface Point {
  x: number;
  y: number;
}

/**
 * A layered left-to-right layout.
 *
 * Levels come from the dependency graph (a node sits one column right of its deepest
 * parent), and the order inside a level comes from the average position of its parents —
 * one barycenter pass, which removes most crossings without pretending to be a
 * Sugiyama implementation. Cycles are illegal in a saved graph but a draft can contain
 * one for a moment, so nodes that never reach in-degree zero are appended in their
 * existing order rather than dropped or looped over forever.
 */
export function arrangeGraph(
  steps: PipelineStepDef[],
  edges: EdgePayload[],
): Record<string, Point> {
  const keys = steps.map((step) => step.key);
  const known = new Set(keys);
  const children = new Map<string, string[]>();
  const parents = new Map<string, string[]>();
  const indegree = new Map<string, number>();

  for (const key of keys) {
    children.set(key, []);
    parents.set(key, []);
    indegree.set(key, 0);
  }
  for (const edge of edges) {
    if (!known.has(edge.from_key) || !known.has(edge.to_key)) continue;
    if (edge.from_key === edge.to_key) continue;
    children.get(edge.from_key)!.push(edge.to_key);
    parents.get(edge.to_key)!.push(edge.from_key);
    indegree.set(edge.to_key, (indegree.get(edge.to_key) ?? 0) + 1);
  }

  // Longest-path layering via Kahn: level(child) = max(level(parent)) + 1.
  const level = new Map<string, number>(keys.map((key) => [key, 0]));
  const queue = keys.filter((key) => (indegree.get(key) ?? 0) === 0);
  const settled = new Set<string>();
  while (queue.length) {
    const key = queue.shift()!;
    if (settled.has(key)) continue;
    settled.add(key);
    for (const child of children.get(key) ?? []) {
      level.set(child, Math.max(level.get(child) ?? 0, (level.get(key) ?? 0) + 1));
      const remaining = (indegree.get(child) ?? 0) - 1;
      indegree.set(child, remaining);
      if (remaining <= 0) queue.push(child);
    }
  }
  // Whatever is left belongs to a cycle: keep it visible, one level past its parents.
  for (const key of keys) {
    if (settled.has(key)) continue;
    const parentLevels = (parents.get(key) ?? []).map((parent) => level.get(parent) ?? 0);
    level.set(key, parentLevels.length ? Math.max(...parentLevels) + 1 : 0);
  }

  const layers: string[][] = [];
  for (const key of keys) {
    const depth = level.get(key) ?? 0;
    (layers[depth] ??= []).push(key);
  }

  // Barycenter ordering: forward using parents, then backward using children, twice.
  // Positions are looked up as an index inside the neighbouring layer.
  const indexIn = (layer: string[], key: string) => layer.indexOf(key);
  for (let pass = 0; pass < 2; pass += 1) {
    for (let depth = 1; depth < layers.length; depth += 1) {
      const previous = layers[depth - 1] ?? [];
      layers[depth] = [...(layers[depth] ?? [])].sort((left, right) => {
        const score = (key: string) => {
          const anchors = (parents.get(key) ?? [])
            .map((parent) => indexIn(previous, parent))
            .filter((index) => index >= 0);
          return anchors.length
            ? anchors.reduce((sum, index) => sum + index, 0) / anchors.length
            : Number.MAX_SAFE_INTEGER;
        };
        return score(left) - score(right);
      });
    }
    for (let depth = layers.length - 2; depth >= 0; depth -= 1) {
      const next = layers[depth + 1] ?? [];
      layers[depth] = [...(layers[depth] ?? [])].sort((left, right) => {
        const score = (key: string) => {
          const anchors = (children.get(key) ?? [])
            .map((child) => indexIn(next, child))
            .filter((index) => index >= 0);
          return anchors.length
            ? anchors.reduce((sum, index) => sum + index, 0) / anchors.length
            : Number.MAX_SAFE_INTEGER;
        };
        return score(left) - score(right);
      });
    }
  }

  const tallest = layers.reduce((max, layer) => Math.max(max, layer?.length ?? 0), 1);
  const positions: Record<string, Point> = {};
  layers.forEach((layer, depth) => {
    if (!layer) return;
    const offset = ((tallest - layer.length) * (NODE_HEIGHT + GAP_Y)) / 2;
    layer.forEach((key, index) => {
      positions[key] = {
        x: 48 + depth * (NODE_WIDTH + GAP_X),
        y: 48 + offset + index * (NODE_HEIGHT + GAP_Y),
      };
    });
  });
  return positions;
}
