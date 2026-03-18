import { useCallback, useEffect, useRef, useState } from 'react';
import Editor, { type Monaco } from '@monaco-editor/react';
import type { editor } from 'monaco-editor';
import { ChevronDown, ChevronUp, Plus, X } from 'lucide-react';
import { useStore } from '../../store';
import { validateSql } from '../../lib/sql/validateSql';
import { getCompletions, type CompletionSuggestion } from '../../lib/sqlCompletions';

const MIN_HEIGHT = 120;
const MAX_HEIGHT = 500;
const DEFAULT_HEIGHT = 250;
const COLLAPSED_HEIGHT = 36;
const TAB_BAR_HEIGHT = 36;
const RESIZE_HANDLE_HEIGHT = 4;

/** Map our SuggestionKind to Monaco CompletionItemKind */
function toMonacoKind(kind: CompletionSuggestion['kind'], monaco: Monaco): number {
  switch (kind) {
    case 'keyword': return monaco.languages.CompletionItemKind.Keyword;
    case 'source': return monaco.languages.CompletionItemKind.Class;
    case 'sink': return monaco.languages.CompletionItemKind.Class;
    case 'field': return monaco.languages.CompletionItemKind.Field;
    case 'function': return monaco.languages.CompletionItemKind.Function;
    case 'snippet': return monaco.languages.CompletionItemKind.Snippet;
    case 'operator': return monaco.languages.CompletionItemKind.Operator;
    default: return monaco.languages.CompletionItemKind.Text;
  }
}

export default function QueryPanel() {
  const queries = useStore((s) => s.queries);
  const selectedQueryId = useStore((s) => s.selectedQueryId);
  const addQuery = useStore((s) => s.addQuery);
  const removeQuery = useStore((s) => s.removeQuery);
  const updateQuery = useStore((s) => s.updateQuery);
  const selectQuery = useStore((s) => s.selectQuery);
  const darkMode = useStore((s) => s.darkMode);

  const [height, setHeight] = useState(DEFAULT_HEIGHT);
  const [collapsed, setCollapsed] = useState(false);
  const [editingTabId, setEditingTabId] = useState<string | null>(null);
  const [editingName, setEditingName] = useState('');
  const prevHeightRef = useRef(DEFAULT_HEIGHT);
  const resizingRef = useRef(false);
  const monacoRef = useRef<Monaco | null>(null);
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const validationTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const selectedQuery = queries.find((q) => q.id === selectedQueryId) ?? null;

  const handleResizeStart = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      resizingRef.current = true;
      const startY = e.clientY;
      const startHeight = collapsed ? COLLAPSED_HEIGHT : height;

      const onMouseMove = (ev: MouseEvent) => {
        if (!resizingRef.current) return;
        const delta = startY - ev.clientY;
        const newHeight = Math.min(MAX_HEIGHT, Math.max(MIN_HEIGHT, startHeight + delta));
        setHeight(newHeight);
        setCollapsed(false);
      };

      const onMouseUp = () => {
        resizingRef.current = false;
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
      };

      document.addEventListener('mousemove', onMouseMove);
      document.addEventListener('mouseup', onMouseUp);
    },
    [collapsed, height],
  );

  const toggleCollapse = useCallback(() => {
    if (collapsed) {
      setCollapsed(false);
      setHeight(prevHeightRef.current);
    } else {
      prevHeightRef.current = height;
      setCollapsed(true);
    }
  }, [collapsed, height]);

  const handleAddQuery = useCallback(() => {
    addQuery();
    // Select the newly added query (last one in the list after state update)
    // We use setTimeout to let zustand update propagate
    setTimeout(() => {
      const state = useStore.getState();
      const last = state.queries[state.queries.length - 1];
      if (last) selectQuery(last.id);
    }, 0);
  }, [addQuery, selectQuery]);

  const handleRemoveQuery = useCallback(
    (id: string) => {
      const idx = queries.findIndex((q) => q.id === id);
      removeQuery(id);
      if (id === selectedQueryId) {
        // Select next or previous or null
        const remaining = queries.filter((q) => q.id !== id);
        if (remaining.length === 0) {
          selectQuery(null);
        } else {
          const nextIdx = Math.min(idx, remaining.length - 1);
          selectQuery(remaining[nextIdx]!.id);
        }
      }
    },
    [queries, selectedQueryId, removeQuery, selectQuery],
  );

  const handleTabDoubleClick = useCallback(
    (id: string) => {
      const q = queries.find((q) => q.id === id);
      if (!q) return;
      setEditingTabId(id);
      setEditingName(q.name);
    },
    [queries],
  );

  const handleNameSubmit = useCallback(
    (id: string) => {
      updateQuery(id, { name: editingName });
      setEditingTabId(null);
    },
    [editingName, updateQuery],
  );

  // Auto-select first query if none selected but queries exist
  useEffect(() => {
    if (!selectedQueryId && queries.length > 0) {
      selectQuery(queries[0]!.id);
    }
  }, [queries, selectedQueryId, selectQuery]);

  // Run debounced SQL validation and set Monaco markers
  const runValidation = useCallback((sql: string) => {
    if (validationTimerRef.current) {
      clearTimeout(validationTimerRef.current);
    }
    validationTimerRef.current = setTimeout(() => {
      const monaco = monacoRef.current;
      const ed = editorRef.current;
      if (!monaco || !ed) return;
      const model = ed.getModel();
      if (!model) return;

      const errors = validateSql(sql);
      const markers = errors.map((err) => ({
        severity: monaco.MarkerSeverity.Error,
        message: err.message,
        startLineNumber: err.line,
        startColumn: err.column + 1,
        endLineNumber: err.line,
        endColumn: err.column + 2,
      }));
      monaco.editor.setModelMarkers(model, 'sql-validation', markers);
    }, 500);
  }, []);

  // Clear markers and completion provider on unmount
  useEffect(() => {
    return () => {
      if (validationTimerRef.current) {
        clearTimeout(validationTimerRef.current);
      }
      completionDisposableRef.current?.dispose();
    };
  }, []);

  // Revalidate when selectedQuery changes
  useEffect(() => {
    if (selectedQuery) {
      runValidation(selectedQuery.sql);
    } else {
      // Clear markers if no query selected
      const monaco = monacoRef.current;
      const ed = editorRef.current;
      if (monaco && ed) {
        const model = ed.getModel();
        if (model) {
          monaco.editor.setModelMarkers(model, 'sql-validation', []);
        }
      }
    }
  }, [selectedQueryId, selectedQuery, runValidation]);

  const completionDisposableRef = useRef<{ dispose: () => void } | null>(null);

  const handleEditorMount = useCallback((ed: editor.IStandaloneCodeEditor, monaco: Monaco) => {
    editorRef.current = ed;
    monacoRef.current = monaco;

    // Dispose previous completion provider if any
    completionDisposableRef.current?.dispose();

    // Register context-aware SQL completion provider
    completionDisposableRef.current = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.', ' '],
      provideCompletionItems(model: editor.ITextModel, position: { lineNumber: number; column: number }) {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        // Get text before cursor for context detection
        const textBeforeCursor = model.getValueInRange({
          startLineNumber: 1, startColumn: 1,
          endLineNumber: position.lineNumber, endColumn: position.column,
        });

        const state = useStore.getState();
        const completions = getCompletions(textBeforeCursor, state.logicalSources, state.sinks);

        return {
          suggestions: completions.map((c) => ({
            label: c.label,
            kind: toMonacoKind(c.kind, monaco),
            insertText: c.insertText,
            insertTextRules: c.isSnippet ? monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet : undefined,
            range,
            detail: c.detail,
            sortText: c.sortText,
            documentation: c.documentation,
          })),
        };
      },
    });

    // Run initial validation
    const model = ed.getModel();
    if (model) {
      runValidation(model.getValue());
    }
  }, [runValidation]);

  const panelHeight = collapsed ? COLLAPSED_HEIGHT : height;
  const editorHeight = panelHeight - TAB_BAR_HEIGHT - RESIZE_HANDLE_HEIGHT;

  return (
    <div
      data-testid="query-panel"
      style={{
        height: panelHeight,
        minHeight: panelHeight,
        display: 'flex',
        flexDirection: 'column',
        borderTop: darkMode ? '1px solid #374151' : '1px solid #e2e8f0',
        background: darkMode ? '#1f2937' : '#ffffff',
      }}
    >
      {/* Resize handle */}
      <div
        data-testid="resize-handle"
        onMouseDown={handleResizeStart}
        style={{
          height: RESIZE_HANDLE_HEIGHT,
          cursor: 'row-resize',
          background: darkMode ? '#374151' : '#f1f5f9',
          flexShrink: 0,
        }}
      />

      {/* Tab bar */}
      <div
        style={{
          height: TAB_BAR_HEIGHT,
          minHeight: TAB_BAR_HEIGHT,
          display: 'flex',
          alignItems: 'center',
          borderBottom: darkMode ? '1px solid #374151' : '1px solid #e2e8f0',
          background: darkMode ? '#1f2937' : '#f8fafc',
          overflow: 'hidden',
          flexShrink: 0,
        }}
      >
        {/* Collapse toggle */}
        <button
          data-testid="collapse-toggle"
          onClick={toggleCollapse}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 28,
            height: 28,
            border: 'none',
            background: 'transparent',
            cursor: 'pointer',
            color: darkMode ? '#9ca3af' : '#64748b',
            flexShrink: 0,
            marginLeft: 4,
          }}
          title={collapsed ? 'Expand' : 'Collapse'}
        >
          {collapsed ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        {/* Scrollable tabs */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            overflow: 'auto',
            flex: 1,
            gap: 0,
          }}
        >
          {queries.map((q) => (
            <div
              key={q.id}
              data-testid={`query-tab-${q.id}`}
              onClick={() => selectQuery(q.id)}
              onDoubleClick={() => handleTabDoubleClick(q.id)}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 4,
                padding: '0 12px',
                height: TAB_BAR_HEIGHT,
                cursor: 'pointer',
                fontSize: 13,
                whiteSpace: 'nowrap',
                borderBottom:
                  q.id === selectedQueryId
                    ? '2px solid #3b82f6'
                    : '2px solid transparent',
                background: q.id === selectedQueryId
                  ? (darkMode ? '#374151' : '#ffffff')
                  : (darkMode ? '#1f2937' : '#f8fafc'),
                flexShrink: 0,
              }}
            >
              {editingTabId === q.id ? (
                <input
                  data-testid="tab-name-input"
                  autoFocus
                  value={editingName}
                  onChange={(e) => setEditingName(e.target.value)}
                  onBlur={() => handleNameSubmit(q.id)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleNameSubmit(q.id);
                  }}
                  style={{
                    fontSize: 13,
                    border: '1px solid #cbd5e1',
                    borderRadius: 3,
                    padding: '1px 4px',
                    width: 100,
                    outline: 'none',
                  }}
                />
              ) : (
                <span
                  style={{
                    fontStyle: q.name ? 'normal' : 'italic',
                    color: q.name
                      ? (darkMode ? '#e5e7eb' : '#334155')
                      : (darkMode ? '#6b7280' : '#94a3b8'),
                  }}
                >
                  {q.name || '(unnamed)'}
                </span>
              )}
              <button
                data-testid={`remove-query-${q.id}`}
                onClick={(e) => {
                  e.stopPropagation();
                  handleRemoveQuery(q.id);
                }}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: 18,
                  height: 18,
                  border: 'none',
                  background: 'transparent',
                  cursor: 'pointer',
                  color: '#94a3b8',
                  borderRadius: 3,
                  padding: 0,
                }}
                title="Remove query"
              >
                <X size={14} />
              </button>
            </div>
          ))}
        </div>

        {/* Add button */}
        <button
          data-testid="add-query"
          onClick={handleAddQuery}
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            width: 28,
            height: 28,
            border: 'none',
            background: 'transparent',
            cursor: 'pointer',
            color: darkMode ? '#9ca3af' : '#64748b',
            flexShrink: 0,
            marginRight: 4,
          }}
          title="Add query"
        >
          <Plus size={16} />
        </button>
      </div>

      {/* Editor area */}
      {!collapsed && (
        <div style={{ flex: 1, minHeight: 0 }}>
          {selectedQuery ? (
            <Editor
              defaultLanguage="sql"
              value={selectedQuery.sql}
              theme={darkMode ? 'vs-dark' : 'vs'}
              onMount={handleEditorMount}
              onChange={(v) => {
                const sql = v ?? '';
                updateQuery(selectedQuery.id, { sql });
                runValidation(sql);
              }}
              options={{
                minimap: { enabled: false },
                lineNumbers: 'on',
                fontSize: 13,
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                automaticLayout: true,
                wordBasedSuggestions: 'off',
              }}
              loading={
                <div style={{ padding: 16, color: '#94a3b8' }}>
                  Loading editor...
                </div>
              }
              height={Math.max(0, editorHeight)}
            />
          ) : (
            <div
              data-testid="empty-state"
              style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                height: '100%',
                color: '#94a3b8',
                gap: 8,
              }}
            >
              <span>Add a query to get started</span>
              <button
                data-testid="empty-add-query"
                onClick={handleAddQuery}
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 4,
                  padding: '6px 12px',
                  border: darkMode ? '1px solid #4b5563' : '1px solid #cbd5e1',
                  borderRadius: 6,
                  background: darkMode ? '#374151' : 'white',
                  cursor: 'pointer',
                  fontSize: 13,
                  color: darkMode ? '#e5e7eb' : '#334155',
                }}
              >
                <Plus size={14} /> Add Query
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
