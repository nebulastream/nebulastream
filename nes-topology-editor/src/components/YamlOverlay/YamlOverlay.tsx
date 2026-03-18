import React, { useEffect, useRef, useCallback } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type { editor as monacoEditor } from 'monaco-editor';
import { X, AlertTriangle } from 'lucide-react';
import { useStore } from '../../store';
import { storeToYaml, yamlToStore } from '../../lib/yaml';

type MonacoInstance = typeof import('monaco-editor');

const DEBOUNCE_MS = 500;

const YamlOverlay: React.FC = () => {
  const visible = useStore((s) => s.yamlOverlayVisible);
  const toggleYamlOverlay = useStore((s) => s.toggleYamlOverlay);
  const replaceTopology = useStore((s) => s.replaceTopology);
  const replaceQueries = useStore((s) => s.replaceQueries);
  const darkMode = useStore((s) => s.darkMode);
  const semanticError = useStore((s) => s.semanticError);

  const workers = useStore((s) => s.workers);
  const logicalSources = useStore((s) => s.logicalSources);
  const physicalSources = useStore((s) => s.physicalSources);
  const sinks = useStore((s) => s.sinks);
  const queries = useStore((s) => s.queries);

  const editorRef = useRef<monacoEditor.IStandaloneCodeEditor | null>(null);
  const monacoRef = useRef<MonacoInstance | null>(null);
  const isFromYamlEdit = useRef(false);
  const debounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const handleEditorMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
  }, []);

  // Store -> YAML direction: update editor when store changes
  useEffect(() => {
    if (!visible || !editorRef.current) return;

    if (isFromYamlEdit.current) {
      isFromYamlEdit.current = false;
      return;
    }

    const newYaml = storeToYaml(
      workers,
      logicalSources,
      physicalSources,
      sinks,
      queries,
    );

    const model = editorRef.current.getModel();
    if (model) {
      const currentValue = model.getValue();
      if (currentValue !== newYaml) {
        model.setValue(newYaml);
      }
    }
  }, [visible, workers, logicalSources, physicalSources, sinks, queries]);

  // YAML -> Store direction: parse edits with debounce
  const handleEditorChange = useCallback(
    (value: string | undefined) => {
      if (debounceTimer.current) {
        clearTimeout(debounceTimer.current);
      }

      debounceTimer.current = setTimeout(() => {
        if (!value || !monacoRef.current || !editorRef.current) return;

        const model = editorRef.current.getModel();
        if (!model) return;

        try {
          const parsed = yamlToStore(value);
          // Clear any error markers on successful parse
          monacoRef.current.editor.setModelMarkers(model, 'yaml', []);

          // Set echo prevention flag before updating store
          isFromYamlEdit.current = true;
          replaceTopology({
            workers: parsed.workers,
            logicalSources: parsed.logicalSources,
            physicalSources: parsed.physicalSources,
            sinks: parsed.sinks,
          });
          replaceQueries(parsed.queries);
        } catch (err: unknown) {
          // Show YAML parse errors as markers
          const yamlErr = err as { mark?: { line?: number; column?: number }; message?: string };
          const line = (yamlErr.mark?.line ?? 0) + 1;
          const column = (yamlErr.mark?.column ?? 0) + 1;

          monacoRef.current.editor.setModelMarkers(model, 'yaml', [
            {
              severity: monacoRef.current.MarkerSeverity.Error,
              message: yamlErr.message ?? 'Invalid YAML',
              startLineNumber: line,
              startColumn: column,
              endLineNumber: line,
              endColumn: column + 1,
            },
          ]);
        }
      }, DEBOUNCE_MS);
    },
    [replaceTopology, replaceQueries],
  );

  // Close on Escape key
  useEffect(() => {
    if (!visible) return;
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        toggleYamlOverlay();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [visible, toggleYamlOverlay]);

  // Cleanup debounce on unmount
  useEffect(() => {
    return () => {
      if (debounceTimer.current) {
        clearTimeout(debounceTimer.current);
      }
    };
  }, []);

  if (!visible) return null;

  // Generate initial YAML content
  const initialYaml = storeToYaml(
    workers,
    logicalSources,
    physicalSources,
    sinks,
    queries,
  );

  return (
    <div
      style={{
        position: 'fixed',
        inset: 0,
        zIndex: 499,
        background: 'rgba(0,0,0,0.25)',
      }}
      onClick={toggleYamlOverlay}
    >
    <div
      style={{
        position: 'absolute',
        right: 16,
        top: 16,
        width: 500,
        height: '70vh',
        zIndex: 500,
        background: darkMode ? '#1f2937' : '#ffffff',
        border: darkMode ? '1px solid #374151' : '1px solid #e0e0e0',
        borderRadius: 8,
        boxShadow: darkMode ? '0 4px 12px rgba(0,0,0,0.4)' : '0 4px 12px rgba(0,0,0,0.15)',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
      }}
      onClick={(e) => e.stopPropagation()}
    >
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '8px 12px',
          borderBottom: darkMode ? '1px solid #374151' : '1px solid #e0e0e0',
          background: darkMode ? '#374151' : '#f8f9fa',
          flexShrink: 0,
        }}
      >
        <span style={{ fontWeight: 600, fontSize: 14, color: darkMode ? '#f3f4f6' : undefined }}>YAML</span>
        <button
          onClick={toggleYamlOverlay}
          title="Close YAML overlay"
          style={{
            background: 'none',
            border: 'none',
            cursor: 'pointer',
            padding: 4,
            display: 'flex',
            alignItems: 'center',
          }}
        >
          <X size={16} />
        </button>
      </div>
      <div style={{ flex: 1, minHeight: 0 }}>
        <Editor
          defaultLanguage="yaml"
          defaultValue={initialYaml}
          theme={darkMode ? 'vs-dark' : 'light'}
          onMount={handleEditorMount}
          onChange={handleEditorChange}
          options={{
            minimap: { enabled: false },
            wordWrap: 'on',
            scrollBeyondLastLine: false,
            fontSize: 13,
            lineNumbers: 'on',
            tabSize: 2,
          }}
        />
      </div>
      {semanticError && semanticError.length > 0 && (
        <div
          style={{
            flexShrink: 0,
            padding: '6px 10px',
            borderTop: darkMode ? '1px solid #374151' : '1px solid #e0e0e0',
            borderLeft: '3px solid #f59e0b',
            background: darkMode ? '#1c1917' : '#fffbeb',
            fontSize: 12,
            fontFamily: 'monospace',
            color: darkMode ? '#fbbf24' : '#92400e',
            maxHeight: 80,
            overflowY: 'auto',
            display: 'flex',
            alignItems: 'flex-start',
            gap: 6,
          }}
        >
          <AlertTriangle size={14} style={{ flexShrink: 0, marginTop: 1 }} color="#f59e0b" />
          <span style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{semanticError}</span>
        </div>
      )}
    </div>
    </div>
  );
};

export default YamlOverlay;
