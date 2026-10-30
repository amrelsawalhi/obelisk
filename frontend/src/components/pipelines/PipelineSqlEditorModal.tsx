import React, { useEffect, useMemo, useRef, useState } from 'react';
import Editor from '@monaco-editor/react';
import {
  AlertTriangle,
  BookOpen,
  Braces,
  Check,
  Copy,
  Workflow,
  X,
} from 'lucide-react';
import type { CatalogNode } from '../../lib/types';
import { formatSql } from '../../lib/sqlFormatter';
import {
  collectSqlAliases,
  collectSqlCatalog,
  uniqueTableNames,
} from '../../lib/sqlCatalog';
import {
  buildParentTokens,
  buildPipelineSqlSnippets,
  type SqlEditorToken,
} from '../../lib/sqlSnippets';
import type { StepOutputDescriptor } from '../../lib/pipelineOutputs';

interface PipelineSqlEditorModalProps {
  open: boolean;
  stepName: string;
  initialSql: string;
  parentOutputs: Array<{ stepName: string; outputs: StepOutputDescriptor[] }>;
  catalogNodes: CatalogNode[];
  onApply: (sql: string) => void;
  onClose: () => void;
}

interface UnresolvedReference {
  text: string;
  index: number;
  length: number;
}

const STEP_REFERENCE = /\{\{\s*steps\.([A-Za-z0-9_-]+)\.([A-Za-z0-9_]+)\s*\}\}/g;

function unresolvedStepReferences(sql: string, tokens: SqlEditorToken[]): UnresolvedReference[] {
  const known = new Set(tokens.map((token) => token.token.replace(/\s+/g, '')));
  const unresolved: UnresolvedReference[] = [];
  for (const match of sql.matchAll(STEP_REFERENCE)) {
    const text = match[0];
    if (!known.has(text.replace(/\s+/g, ''))) {
      unresolved.push({ text, index: match.index ?? 0, length: text.length });
    }
  }
  return unresolved;
}

const countLabel = (value: number, one: string, many: string) =>
  `${value} ${value === 1 ? one : many}`;

const PipelineSqlEditorModal: React.FC<PipelineSqlEditorModalProps> = ({
  open,
  stepName,
  initialSql,
  parentOutputs,
  catalogNodes,
  onApply,
  onClose,
}) => {
  const [draft, setDraft] = useState(initialSql);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  const [editorReady, setEditorReady] = useState(false);
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<any>(null);
  const providerRef = useRef<{ dispose: () => void } | null>(null);
  const formatterRef = useRef<{ dispose: () => void } | null>(null);
  const draftRef = useRef(draft);
  const applyRef = useRef<() => void>(() => {});
  const closeRef = useRef<() => void>(() => {});
  const wasOpen = useRef(false);

  const catalog = useMemo(() => collectSqlCatalog(catalogNodes), [catalogNodes]);
  const catalogRef = useRef(catalog);
  const tokens = useMemo(() => buildParentTokens(parentOutputs), [parentOutputs]);
  const tokensRef = useRef(tokens);
  const snippets = useMemo(() => buildPipelineSqlSnippets(tokens), [tokens]);
  const snippetsRef = useRef(snippets);
  const unresolved = useMemo(
    () => unresolvedStepReferences(draft, tokens),
    [draft, tokens],
  );

  catalogRef.current = catalog;
  tokensRef.current = tokens;
  snippetsRef.current = snippets;
  draftRef.current = draft;

  useEffect(() => {
    if (open && !wasOpen.current) {
      setDraft(initialSql);
      wasOpen.current = true;
    }
    if (!open) {
      wasOpen.current = false;
      setEditorReady(false);
      editorRef.current = null;
      monacoRef.current = null;
    }
  }, [open, initialSql]);

  useEffect(() => {
    if (!open) return;
    const previous = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    return () => {
      document.body.style.overflow = previous;
    };
  }, [open]);

  useEffect(() => {
    if (!editorReady || !monacoRef.current || !editorRef.current) return;
    const monaco = monacoRef.current;
    const model = editorRef.current.getModel?.();
    if (!model) return;
    const markers = unresolved.map((reference) => {
      const start = model.getPositionAt(reference.index);
      const end = model.getPositionAt(reference.index + reference.length);
      return {
        startLineNumber: start.lineNumber,
        startColumn: start.column,
        endLineNumber: end.lineNumber,
        endColumn: end.column,
        message: `No direct parent publishes ${reference.text}. Connect the parent or fix the step name.`,
        severity: monaco.MarkerSeverity.Warning,
        source: 'pipeline-sql',
      };
    });
    monaco.editor.setModelMarkers(model, 'pipeline-sql', markers);
    return () => monaco.editor.setModelMarkers(model, 'pipeline-sql', []);
  }, [editorReady, unresolved]);

  const apply = () => {
    onApply(draftRef.current);
    onClose();
  };
  applyRef.current = apply;
  closeRef.current = onClose;

  const format = () => {
    const formatted = formatSql(draftRef.current);
    setDraft(formatted);
    const editor = editorRef.current;
    if (editor) {
      const model = editor.getModel?.();
      if (model) {
        editor.executeEdits('pipeline-sql-format', [
          { range: model.getFullModelRange(), text: formatted, forceMoveMarkers: true },
        ]);
      }
      editor.focus();
    }
  };

  const insertText = (text: string, asSnippet = false) => {
    const editor = editorRef.current;
    if (!editor) {
      setDraft((current) => `${current}${current.endsWith('\n') ? '' : '\n'}${text}`);
      return;
    }
    editor.focus();
    if (asSnippet) {
      const controller = editor.getContribution?.('snippetController2') as
        | { insert?: (snippet: string) => void }
        | undefined;
      if (controller?.insert) {
        controller.insert(text);
        return;
      }
    }
    const selection = editor.getSelection();
    if (!selection) return;
    editor.executeEdits('pipeline-sql-insert', [
      { range: selection, text, forceMoveMarkers: true },
    ]);
  };

  const copyToken = async (value: string) => {
    try {
      await navigator.clipboard.writeText(value);
      setCopiedToken(value);
      window.setTimeout(() => setCopiedToken(null), 1500);
    } catch {
      setCopiedToken(null);
    }
  };

  const handleMount = (editor: any, monaco: any) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    setEditorReady(true);

    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      applyRef.current();
    });
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyS, () => {
      applyRef.current();
    });
    editor.addCommand(monaco.KeyCode.Escape, () => {
      closeRef.current();
    });
    editor.addAction({
      id: 'obelisk-pipeline-format-sql',
      label: 'Obelisk: Format SQL',
      keybindings: [monaco.KeyMod.Shift | monaco.KeyMod.Alt | monaco.KeyCode.KeyF],
      contextMenuGroupId: 'modification',
      contextMenuOrder: 1,
      run: format,
    });

    providerRef.current?.dispose();
    providerRef.current = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.'],
      provideCompletionItems: (model: any, position: any) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };
        const tableNames = uniqueTableNames(catalogRef.current);
        const tableItems = tableNames.map((name) => ({
          label: name,
          kind: monaco.languages.CompletionItemKind.Struct,
          insertText: name,
          detail: 'Table or view',
          documentation: 'Resolved by the same catalog rewriter as Query Studio.',
          sortText: `0_${name}`,
          range,
        }));

        const before = model.getValueInRange({
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const qualifier = /([A-Za-z_]\w*)\.\w*$/.exec(before)?.[1]?.toLowerCase();
        const aliases = collectSqlAliases(model.getValue(), catalogRef.current);
        const qualifiedTable = qualifier ? aliases.get(qualifier) : undefined;
        const fallbackTable = qualifier
          ? catalogRef.current.find((table) =>
              table.names.some(
                (name) => name.toLowerCase().split('.').pop() === qualifier,
              ),
            )
          : undefined;

        const seenColumns = new Set<string>();
        const columnItems: any[] = [];
        const columnSources = qualifiedTable ?? fallbackTable
          ? [qualifiedTable ?? fallbackTable]
          : catalogRef.current;
        columnSources.forEach((table) => {
          if (!table) return;
          table.columns.forEach((column) => {
            const identity = `${column.name}|${table.names[0]}`;
            if (seenColumns.has(identity)) return;
            seenColumns.add(identity);
            columnItems.push({
              label: column.name,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: column.name,
              detail: `${column.data_type} • ${table.names[0]}`,
              documentation: `${table.detail}\nColumn: ${column.name}\nType: ${column.data_type}`,
              sortText: `1_${column.name}`,
              range,
            });
          });
        });

        const parentItems = tokensRef.current.map((item) => ({
          label: item.token,
          kind: monaco.languages.CompletionItemKind.Reference,
          insertText: item.token,
          detail: `${item.type === 'table' ? 'Parent table' : 'Parent value'} • ${item.stepName}`,
          documentation: `Output ${item.output} from direct parent ${item.stepName}.`,
          sortText: `2_${item.stepName}_${item.output}`,
          range,
        }));

        const snippetItems = snippetsRef.current.map((snippet) => ({
          label: snippet.label,
          kind: monaco.languages.CompletionItemKind.Snippet,
          insertText: snippet.sql,
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          detail: 'Pipeline SQL pattern',
          documentation: snippet.description,
          sortText: `3_${snippet.label}`,
          range,
        }));

        const functionItems = [
          ['COUNT(*)', 'COUNT(*)'],
          ['COUNT', 'COUNT(${1:column})'],
          ['SUM', 'SUM(${1:column})'],
          ['AVG', 'AVG(${1:column})'],
          ['COALESCE', 'COALESCE(${1:value}, ${2:fallback})'],
          ['DATE_TRUNC', "DATE_TRUNC('${1:day}', ${2:timestamp})"],
          ['ROW_NUMBER', 'ROW_NUMBER() OVER (PARTITION BY ${1:key} ORDER BY ${2:value})'],
        ].map(([label, insertText]) => ({
          label,
          kind: monaco.languages.CompletionItemKind.Function,
          insertText,
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          detail: 'DuckDB function',
          sortText: `4_${label}`,
          range,
        }));

        const seenSuggestions = new Set<string>();
        return {
          suggestions: [
            ...tableItems,
            ...columnItems,
            ...parentItems,
            ...snippetItems,
            ...functionItems,
          ].filter((item) => {
            const key = `${item.kind}:${item.label}:${item.detail ?? ''}`;
            if (seenSuggestions.has(key)) return false;
            seenSuggestions.add(key);
            return true;
          }),
        };
      },
    });

    formatterRef.current?.dispose();
    formatterRef.current = monaco.languages.registerDocumentFormattingEditProvider('sql', {
      provideDocumentFormattingEdits: (model: any) => [
        {
          range: model.getFullModelRange(),
          text: formatSql(model.getValue()),
        },
      ],
    });
  };

  useEffect(
    () => () => {
      providerRef.current?.dispose();
      providerRef.current = null;
      formatterRef.current?.dispose();
      formatterRef.current = null;
    },
    [open],
  );

  if (!open) return null;

  const dirty = draft !== initialSql;
  const lineCount = draft ? draft.split('\n').length : 0;
  const unresolvedNames = Array.from(new Set(unresolved.map((item) => item.text)));

  return (
    <div
      className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-900/50 p-3 backdrop-blur-xs"
      role="presentation"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        className="flex h-[90vh] w-[94vw] max-w-[1600px] flex-col overflow-hidden rounded-xl border border-slate-200 bg-white shadow-2xl"
        role="dialog"
        aria-modal="true"
        aria-labelledby="pipeline-sql-editor-title"
      >
        <header className="flex shrink-0 items-center justify-between border-b border-slate-200 bg-slate-50/80 px-4 py-3">
          <div className="flex min-w-0 items-center gap-2.5">
            <div className="rounded-lg bg-sky-100 p-1.5 text-sky-700">
              <Braces className="h-4 w-4" />
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <h2 id="pipeline-sql-editor-title" className="truncate text-sm font-bold text-slate-900">
                  Raw SQL
                </h2>
                <span className="rounded-full border border-slate-200 bg-white px-2 py-0.5 font-mono text-[10px] text-slate-500">
                  {stepName}
                </span>
                {dirty && (
                  <span className="rounded-full bg-amber-100 px-2 py-0.5 text-[10px] font-semibold text-amber-800">
                    Unsaved
                  </span>
                )}
              </div>
              <p className="mt-0.5 text-[11px] text-slate-500">
                Query tables and use direct-parent outputs. Ctrl/Cmd+Enter applies.
              </p>
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1.5 text-slate-400 transition-colors hover:bg-slate-200/60 hover:text-slate-700"
            aria-label="Close SQL editor"
          >
            <X className="h-4 w-4" />
          </button>
        </header>

        <div className="flex min-h-0 flex-1 flex-col md:flex-row">
          <section className="flex min-h-0 min-w-0 flex-1 flex-col">
            <div className="flex shrink-0 items-center justify-between border-b border-slate-100 bg-white px-3 py-1.5">
              <span className="font-mono text-[10px] text-slate-400">
                {countLabel(lineCount, 'line', 'lines')} · {draft.length.toLocaleString()} chars
              </span>
              <button
                type="button"
                onClick={format}
                className="flex items-center gap-1 rounded-md border border-slate-200 px-2 py-1 text-[10px] font-semibold text-slate-600 transition-colors hover:border-sky-200 hover:bg-sky-50 hover:text-sky-700"
              >
                <Braces className="h-3 w-3" />
                Format
              </button>
            </div>
            <div className="min-h-0 flex-1">
              <Editor
                height="100%"
                defaultLanguage="sql"
                language="sql"
                theme="light"
                value={draft}
                onChange={(value) => setDraft(value ?? '')}
                onMount={handleMount}
                path={`pipeline-sql-${stepName}.sql`}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  fontFamily: 'JetBrains Mono, Fira Code, Menlo, monospace',
                  lineNumbers: 'on',
                  scrollBeyondLastLine: false,
                  wordWrap: 'on',
                  automaticLayout: true,
                  padding: { top: 12, bottom: 12 },
                  quickSuggestions: { other: true, comments: false, strings: true },
                  suggestOnTriggerCharacters: true,
                  acceptSuggestionOnEnter: 'smart',
                  tabCompletion: 'on',
                  snippetSuggestions: 'top',
                  parameterHints: { enabled: true },
                  folding: true,
                  matchBrackets: 'always',
                  autoClosingBrackets: 'always',
                  autoClosingQuotes: 'always',
                  bracketPairColorization: { enabled: true },
                  guides: { bracketPairs: true, indentation: true },
                  cursorBlinking: 'smooth',
                  smoothScrolling: true,
                  formatOnPaste: true,
                  stickyScroll: { enabled: false },
                }}
                loading={
                  <div className="flex h-full items-center justify-center text-xs text-slate-400">
                    Loading SQL editor…
                  </div>
                }
              />
            </div>
          </section>

          <aside className="flex min-h-0 w-full shrink-0 flex-col border-t border-slate-200 bg-slate-50/60 md:w-80 md:border-l md:border-t-0">
            <div className="min-h-0 flex-1 space-y-4 overflow-y-auto p-3">
              <section>
                <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wide text-slate-500">
                  <Workflow className="h-3.5 w-3.5" />
                  Parent outputs
                </div>
                {tokens.length ? (
                  <div className="space-y-1">
                    {tokens.map((item) => (
                      <div
                        key={item.token}
                        className="group flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-2 py-1.5"
                      >
                        <button
                          type="button"
                          onClick={() => insertText(item.token)}
                          className="min-w-0 flex-1 truncate text-left font-mono text-[10px] text-sky-800 hover:text-sky-950"
                          title={`Insert ${item.token}`}
                        >
                          {item.token}
                        </button>
                        <span className="shrink-0 rounded bg-slate-100 px-1 text-[8px] font-bold uppercase text-slate-400">
                          {item.type}
                        </span>
                        <button
                          type="button"
                          onClick={() => void copyToken(item.token)}
                          className="shrink-0 rounded p-0.5 text-slate-300 opacity-0 transition-opacity hover:text-slate-600 group-hover:opacity-100"
                          aria-label={`Copy ${item.token}`}
                        >
                          <Copy className="h-3 w-3" />
                        </button>
                      </div>
                    ))}
                    {copiedToken && (
                      <p className="text-[9px] text-emerald-600">Copied {copiedToken}</p>
                    )}
                    {tokens.some((item) => item.type === 'table') && (
                      <p className="pt-1 text-[9px] leading-snug text-slate-400">
                        Batch ingest steps may publish a wildcard table such as raw.*. Use its
                        rows output with explicit table names in that case.
                      </p>
                    )}
                  </div>
                ) : (
                  <p className="rounded-lg border border-dashed border-slate-200 bg-white/70 p-2 text-[10px] leading-snug text-slate-400">
                    Connect a parent step to make its table and scalar outputs available here.
                  </p>
                )}
              </section>

              <section>
                <div className="mb-1.5 flex items-center gap-1.5 text-[10px] font-bold uppercase tracking-wide text-slate-500">
                  <BookOpen className="h-3.5 w-3.5" />
                  SQL snippets
                </div>
                <div className="space-y-1.5">
                  {snippets.map((snippet) => (
                    <button
                      key={snippet.id}
                      type="button"
                      onClick={() => insertText(snippet.sql, true)}
                      className="w-full rounded-lg border border-slate-200 bg-white px-2.5 py-2 text-left transition-all hover:border-sky-200 hover:bg-sky-50/60 hover:shadow-xs"
                    >
                      <span className="block text-[11px] font-semibold text-slate-700">
                        {snippet.label}
                      </span>
                      <span className="mt-0.5 block text-[10px] leading-snug text-slate-400">
                        {snippet.description}
                      </span>
                    </button>
                  ))}
                </div>
              </section>
            </div>
          </aside>
        </div>

        <footer className="shrink-0 border-t border-slate-200 bg-white px-4 py-2.5">
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0 text-[10px] leading-snug">
              {unresolvedNames.length ? (
                <p className="flex items-center gap-1.5 text-amber-700" title={unresolvedNames.join(', ')}>
                  <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                  <span className="truncate">
                    {countLabel(unresolvedNames.length, 'unresolved reference', 'unresolved references')}:
                    {' '}
                    {unresolvedNames.join(', ')}
                  </span>
                </p>
              ) : (
                <p className="flex items-center gap-1.5 text-emerald-700">
                  <Check className="h-3.5 w-3.5" />
                  All step references resolve to direct parents.
                </p>
              )}
              <p className="mt-0.5 text-slate-400">
                Applying updates the step draft only; it does not execute SQL.
              </p>
            </div>
            <div className="flex shrink-0 items-center gap-2">
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg border border-slate-200 px-3 py-1.5 text-xs font-semibold text-slate-600 transition-colors hover:bg-slate-50"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={apply}
                disabled={!draft.trim()}
                className="flex items-center gap-1.5 rounded-lg bg-sky-600 px-3.5 py-1.5 text-xs font-semibold text-white shadow-xs transition-colors hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-40"
              >
                <Check className="h-3.5 w-3.5" />
                Apply SQL
              </button>
            </div>
          </div>
        </footer>
      </div>
    </div>
  );
};

export default PipelineSqlEditorModal;
