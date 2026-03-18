import { SINK_CONFIGS, SOURCE_CONFIGS, PARSER_CONFIG, type FormFieldDef } from './sourceConfigs';
import type { LogicalSource, Sink } from './types';

export const SQL_KEYWORDS = [
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL',
  'AS', 'ON', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS',
  'GROUP', 'BY', 'ORDER', 'ASC', 'DESC', 'HAVING', 'LIMIT', 'OFFSET',
  'UNION', 'ALL', 'DISTINCT', 'BETWEEN', 'LIKE', 'EXISTS', 'INTO',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'TRUE', 'FALSE', 'CAST', 'WINDOW', 'TUMBLING', 'SLIDING', 'EMIT',
];

const AGGREGATE_FUNCTIONS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];

export type SuggestionKind = 'keyword' | 'source' | 'sink' | 'field' | 'function' | 'snippet' | 'operator';

export interface CompletionSuggestion {
  label: string;
  kind: SuggestionKind;
  insertText: string;
  isSnippet?: boolean;
  detail?: string;
  sortText?: string;
  documentation?: string;
}

export type CompletionContext =
  | 'afterDot'
  | 'afterFrom'
  | 'afterInto'
  | 'afterSelect'
  | 'afterWhere'
  | 'default';

/** Determine completion context from text before the cursor. */
export function detectContext(textBeforeCursor: string): { context: CompletionContext; dotPrefix?: string } {
  const upper = textBeforeCursor.toUpperCase().trimEnd();

  // Check dot-qualified access first
  const dotMatch = textBeforeCursor.match(/(\w+)\.$/);
  if (dotMatch) {
    return { context: 'afterDot', dotPrefix: dotMatch[1]! };
  }

  if (/\b(FROM|JOIN)\s+\w*$/i.test(textBeforeCursor)) return { context: 'afterFrom' };
  if (/\bINTO\s+\w*$/i.test(textBeforeCursor)) return { context: 'afterInto' };
  if (/\bSELECT\s+([\w,\s*]*,\s*)?\w*$/i.test(textBeforeCursor) && !upper.includes('FROM')) {
    return { context: 'afterSelect' };
  }
  if (/\b(WHERE|AND|OR|ON|HAVING)\s+\w*$/i.test(textBeforeCursor)) return { context: 'afterWhere' };

  return { context: 'default' };
}

/**
 * Build a snippet string for an inline source/sink with tab stops.
 * @param kind 'SOURCE' or 'SINK'
 * @param typeName e.g. 'File', 'Print', 'Generator'
 * @param fields config field definitions
 * @param extraFields additional snippet params prepended (e.g. HOST)
 */
export function buildInlineSnippet(
  kind: 'SOURCE' | 'SINK',
  typeName: string,
  fields: FormFieldDef[],
  extraFields?: { key: string; placeholder: string; quote: boolean }[],
): { text: string; doc: string } {
  const params: string[] = [];
  const docLines: string[] = [`Inline ${typeName} ${kind.toLowerCase()} config:`];
  let tabIdx = 1;

  if (extraFields) {
    for (const ef of extraFields) {
      const configKey = `\`${kind}\`.\`${ef.key}\``;
      const q = ef.quote ? '"' : '';
      params.push(`${q}\${${tabIdx}:${ef.placeholder}}${q} AS ${configKey}`);
      docLines.push(`  ${ef.key}${ef.key === 'HOST' ? ' [required in distributed mode]' : ''}`);
      tabIdx++;
    }
  }

  for (const field of fields) {
    const configKey = `\`${kind}\`.\`${field.key.toUpperCase()}\``;
    const placeholder = field.defaultValue || field.placeholder || field.label;
    const quote = field.type === 'number' ? '' : "'";
    params.push(`${quote}\${${tabIdx}:${placeholder}}${quote} AS ${configKey}`);
    docLines.push(`  ${field.label} (${field.key})${field.required ? ' [required]' : ''}`);
    tabIdx++;
  }

  if (params.length === 0) {
    return { text: `${typeName}($0)`, doc: `${typeName} ${kind.toLowerCase()} with no required config.` };
  }

  return {
    text: `${typeName}(${params.join(', ')})`,
    doc: docLines.join('\n'),
  };
}

/** Build the timestamp choice string for window snippets. */
export function buildTimestampChoice(logicalSources: LogicalSource[]): string {
  const allFields = logicalSources.flatMap((ls) => ls.schema.map((f) => f.name));
  const uniqueFields = [...new Set(allFields)];
  return uniqueFields.length > 0
    ? `\${1|${uniqueFields.join(',')}|}`
    : '${1:timestamp}';
}

/** Get completions for the given context and topology state. */
export function getCompletions(
  textBeforeCursor: string,
  logicalSources: LogicalSource[],
  sinks: Sink[],
): CompletionSuggestion[] {
  const { context, dotPrefix } = detectContext(textBeforeCursor);
  const suggestions: CompletionSuggestion[] = [];

  switch (context) {
    case 'afterDot': {
      const ls = logicalSources.find((s) => s.name.toLowerCase() === dotPrefix!.toLowerCase());
      if (ls) {
        for (const field of ls.schema) {
          suggestions.push({
            label: field.name,
            kind: 'field',
            insertText: field.name,
            detail: `${field.type} — ${ls.name}`,
            sortText: '0' + field.name,
          });
        }
      }
      break;
    }

    case 'afterFrom': {
      for (const ls of logicalSources) {
        suggestions.push({
          label: ls.name,
          kind: 'source',
          insertText: ls.name,
          detail: 'Logical Source',
          sortText: '0' + ls.name,
        });
      }
      // Inline source snippets
      const hostField = { key: 'HOST', placeholder: 'worker:9090', quote: true };
      for (const [srcType, fields] of Object.entries(SOURCE_CONFIGS)) {
        const allFields = [...fields, ...PARSER_CONFIG];
        const snippet = buildInlineSnippet('SOURCE', srcType, allFields, [hostField]);
        suggestions.push({
          label: `${srcType}(...)`,
          kind: 'snippet',
          insertText: snippet.text,
          isSnippet: true,
          detail: `Inline ${srcType} source`,
          sortText: '1' + srcType,
          documentation: snippet.doc,
        });
      }
      break;
    }

    case 'afterInto': {
      for (const sink of sinks) {
        if (sink.name) {
          suggestions.push({
            label: sink.name,
            kind: 'sink',
            insertText: sink.name,
            detail: `Sink (${sink.type})`,
            sortText: '0' + sink.name,
          });
        }
      }
      const sinkHostField = { key: 'HOST', placeholder: 'worker:9090', quote: true };
      for (const [sinkType, fields] of Object.entries(SINK_CONFIGS)) {
        const snippet = buildInlineSnippet('SINK', sinkType, fields, [sinkHostField]);
        suggestions.push({
          label: `${sinkType}(...)`,
          kind: 'snippet',
          insertText: snippet.text,
          isSnippet: true,
          detail: `Inline ${sinkType} sink`,
          sortText: '1' + sinkType,
          documentation: snippet.doc,
        });
        suggestions.push({
          label: `${sinkType}()`,
          kind: 'snippet',
          insertText: `${sinkType}()`,
          detail: `Inline ${sinkType} sink (no config)`,
          sortText: '1' + sinkType + 'z',
        });
      }
      break;
    }

    case 'afterSelect':
    case 'afterWhere': {
      for (const ls of logicalSources) {
        for (const field of ls.schema) {
          suggestions.push({
            label: field.name,
            kind: 'field',
            insertText: field.name,
            detail: `${field.type} — ${ls.name}`,
            sortText: '0' + field.name,
          });
        }
        suggestions.push({
          label: ls.name,
          kind: 'source',
          insertText: ls.name,
          detail: 'Logical Source (type . for fields)',
          sortText: '0z' + ls.name,
        });
      }

      if (context === 'afterSelect') {
        suggestions.push({
          label: '*',
          kind: 'operator',
          insertText: '*',
          detail: 'All columns',
          sortText: '00',
        });
      }

      for (const fn of AGGREGATE_FUNCTIONS) {
        suggestions.push({
          label: fn,
          kind: 'function',
          insertText: `${fn}(\${1:expr})`,
          isSnippet: true,
          detail: 'Aggregate function',
          sortText: '1' + fn,
        });
      }

      addKeywordSuggestions(suggestions);
      break;
    }

    default: {
      addKeywordSuggestions(suggestions);

      for (const ls of logicalSources) {
        suggestions.push({
          label: ls.name,
          kind: 'source',
          insertText: ls.name,
          detail: 'Logical Source',
          sortText: '0' + ls.name,
        });
        for (const field of ls.schema) {
          suggestions.push({
            label: field.name,
            kind: 'field',
            insertText: field.name,
            detail: `${field.type} — ${ls.name}`,
            sortText: '0' + field.name,
          });
        }
      }

      for (const sink of sinks) {
        if (sink.name) {
          suggestions.push({
            label: sink.name,
            kind: 'sink',
            insertText: sink.name,
            detail: 'Sink',
            sortText: '0' + sink.name,
          });
        }
      }

      // Full query snippet
      suggestions.push({
        label: 'SELECT ... FROM ... INTO ...',
        kind: 'snippet',
        insertText: 'SELECT ${1:*} FROM ${2:source} INTO ${3:sink}',
        isSnippet: true,
        detail: 'Full NES query template',
        sortText: '2query',
      });

      // Window snippets
      const tsChoice = buildTimestampChoice(logicalSources);
      suggestions.push({
        label: 'WINDOW TUMBLING',
        kind: 'snippet',
        insertText: `WINDOW TUMBLING(${tsChoice}, SIZE \${2:1} \${3|SEC,MS,MINUTE,HOUR,DAY|})`,
        isSnippet: true,
        detail: 'Tumbling window',
        sortText: '2window-t',
      });
      suggestions.push({
        label: 'WINDOW SLIDING',
        kind: 'snippet',
        insertText: `WINDOW SLIDING(${tsChoice}, SIZE \${2:10} \${3|SEC,MS,MINUTE,HOUR,DAY|}, ADVANCE BY \${4:1} \${5|SEC,MS,MINUTE,HOUR,DAY|})`,
        isSnippet: true,
        detail: 'Sliding window',
        sortText: '2window-s',
      });

      break;
    }
  }

  return suggestions;
}

function addKeywordSuggestions(suggestions: CompletionSuggestion[]) {
  for (const kw of SQL_KEYWORDS) {
    suggestions.push({
      label: kw,
      kind: 'keyword',
      insertText: kw,
      sortText: '1' + kw,
    });
  }
}
