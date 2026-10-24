import { useCallback, useEffect, useRef, useState } from 'react';

const clamp = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value));

/**
 * A persisted pane size driven by a drag handle.
 *
 * Written by hand rather than pulled from a library: this screen needs three panes
 * and a log dock, which is a pointer handler and a number, not a dependency. Sizes
 * survive reloads (``localStorage``) because an operator who widened the inspector to
 * read a long SQL step should not have to do it again tomorrow.
 */
export function useResizableSize(
  storageKey: string,
  initial: number,
  min: number,
  max: number,
): {
  size: number;
  setSize: (value: number) => void;
  startDrag: (event: React.PointerEvent, options?: { invert?: boolean }) => void;
  dragging: boolean;
} {
  const [size, setSizeState] = useState(() => {
    if (typeof window === 'undefined') return initial;
    const stored = Number(window.localStorage.getItem(storageKey));
    return Number.isFinite(stored) && stored > 0 ? clamp(stored, min, max) : initial;
  });
  const [dragging, setDragging] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(storageKey, String(Math.round(size)));
  }, [storageKey, size]);

  const setSize = useCallback(
    (value: number) => setSizeState(clamp(value, min, max)),
    [min, max],
  );

  const startDrag = useCallback(
    (event: React.PointerEvent, options?: { invert?: boolean }) => {
      event.preventDefault();
      event.stopPropagation();
      const startX = event.clientX;
      const startY = event.clientY;
      const startSize = size;
      const horizontal = event.currentTarget instanceof HTMLElement
        ? event.currentTarget.dataset.axis !== 'y'
        : true;
      const previousCursor = document.body.style.cursor;
      document.body.style.cursor = horizontal ? 'col-resize' : 'row-resize';
      setDragging(true);

      const onMove = (moveEvent: PointerEvent) => {
        const delta = horizontal
          ? moveEvent.clientX - startX
          : moveEvent.clientY - startY;
        // A left-hand pane grows with the pointer; a bottom dock grows against it.
        setSizeState(clamp(startSize + (options?.invert ? -delta : delta), min, max));
      };
      const onUp = () => {
        document.body.style.cursor = previousCursor;
        setDragging(false);
        window.removeEventListener('pointermove', onMove);
        window.removeEventListener('pointerup', onUp);
      };
      window.addEventListener('pointermove', onMove);
      window.addEventListener('pointerup', onUp);
    },
    [size, min, max],
  );

  return { size, setSize, startDrag, dragging };
}

/** Resets a pane when a double-click says "I liked the default". */
export function useDragHandleProps(
  startDrag: (event: React.PointerEvent, options?: { invert?: boolean }) => void,
  reset: () => void,
  options?: { invert?: boolean; axis?: 'x' | 'y' },
) {
  const ref = useRef<HTMLDivElement>(null);
  return {
    ref,
    role: 'separator' as const,
    'aria-orientation': (options?.axis === 'y' ? 'vertical' : 'horizontal') as
      | 'vertical'
      | 'horizontal',
    'data-axis': options?.axis ?? 'x',
    onPointerDown: (event: React.PointerEvent) => startDrag(event, options),
    onDoubleClick: reset,
  };
}
