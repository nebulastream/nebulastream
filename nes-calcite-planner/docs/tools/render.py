#!/usr/bin/env python3
"""render.py <plans.json> <out.html>: per query its SQL, plans, planner steps and a diagram of its pipelines.

Self-contained HTML (no scripts, no external resources). See DumpPlans.java for the input.
"""
import html
import json
import sys
import textwrap

plans = json.load(open(sys.argv[1]))
CHAR, LINE, PAD, GAP_X, GAP_Y, WRAP = 7.2, 17, 12, 70, 26, 58
ROLE = {'SourceScan': 'start', 'Scan': 'start', 'Sink': 'start', 'Emit': 'end'}
COLOR = {'start': '#1f5fbf', 'end': '#b8590a', 'middle': '#4b5563'}


def esc(s):
    return html.escape(s, quote=True)


def step(plan, prefix):
    return next((st['plan'] for st in plan['steps'] if st['name'].startswith(prefix)), '')


def diagram(ops):
    by_id = {o['id']: o for o in ops}
    pipelines = {}
    for o in sorted(ops, key=lambda o: o['id']):
        pipelines.setdefault(o['pipeline'], []).append(o)
    edges = sorted({(by_id[i]['pipeline'], o['pipeline']) for o in ops for i in o['inputs'] if by_id[i]['pipeline'] != o['pipeline']})
    level = {}

    def lvl(p):
        if p not in level:
            preds = [a for a, b in edges if b == p]
            level[p] = 0 if not preds else 1 + max(lvl(a) for a in preds)
        return level[p]

    for p in pipelines:
        lvl(p)

    boxes = {}
    for p, members in pipelines.items():
        lines = [('head', f'pipeline {p}')]
        for o in members:
            title = f"{o['kind']}  {o['text']}".rstrip()
            for k, part in enumerate(textwrap.wrap(title, WRAP) or [title]):
                lines.append(('op' if k == 0 else 'opcont', part, o['kind']))
            for part in textwrap.wrap('→ ' + o['schema'], WRAP):
                lines.append(('schema', part))
        boxes[p] = {'lines': lines, 'w': max(len(l[1]) for l in lines) * CHAR + 2 * PAD, 'h': len(lines) * LINE + 2 * PAD - 4}

    columns = {}
    for p in sorted(pipelines, key=lambda p: (level[p], min(o['id'] for o in pipelines[p]))):
        columns.setdefault(level[p], []).append(p)
    x = 10
    for c in sorted(columns):
        width = max(boxes[p]['w'] for p in columns[c])
        y = 10
        for p in columns[c]:
            boxes[p].update(x=x, y=y, w=width)
            y += boxes[p]['h'] + GAP_Y
        x += width + GAP_X
    width = x - GAP_X + 20
    height = max(b['y'] + b['h'] for b in boxes.values()) + 20

    out = [f'<svg viewBox="0 0 {width:.0f} {height:.0f}" width="{width:.0f}" height="{height:.0f}" role="img" '
           'aria-label="pipelines of the physical plan">',
           '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
           'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#6b7280"/></marker></defs>']
    for a, b in edges:
        src, dst = boxes[a], boxes[b]
        x1, y1, x2, y2 = src['x'] + src['w'], src['y'] + src['h'] / 2, dst['x'], dst['y'] + dst['h'] / 2
        mx = (x1 + x2) / 2
        out.append(f'<path d="M{x1:.1f},{y1:.1f} C{mx:.1f},{y1:.1f} {mx:.1f},{y2:.1f} {x2 - 2:.1f},{y2:.1f}" '
                   'fill="none" stroke="#6b7280" stroke-width="1.5" marker-end="url(#arrow)"/>')
    for p, box in boxes.items():
        out.append(f'<rect x="{box["x"]:.1f}" y="{box["y"]:.1f}" width="{box["w"]:.1f}" height="{box["h"]:.1f}" rx="8" '
                   'fill="#f8fafc" stroke="#cbd5e1"/>')
        ty = box['y'] + PAD + 9
        for line in box['lines']:
            tx = box['x'] + PAD
            if line[0] == 'head':
                out.append(f'<text x="{tx:.1f}" y="{ty:.1f}" class="head">{esc(line[1])}</text>')
            elif line[0] == 'op':
                kind, rest = line[1].split('  ', 1) if '  ' in line[1] else (line[1], '')
                color = COLOR[ROLE.get(line[2], 'middle')]
                out.append(f'<text x="{tx:.1f}" y="{ty:.1f}" class="op"><tspan fill="{color}" font-weight="700">{esc(kind)}</tspan>'
                           f'  {esc(rest)}</text>')
            elif line[0] == 'opcont':
                out.append(f'<text x="{tx:.1f}" y="{ty:.1f}" class="op">{esc(line[1])}</text>')
            else:
                out.append(f'<text x="{tx:.1f}" y="{ty:.1f}" class="schema">{esc(line[1])}</text>')
            ty += LINE
    out.append('</svg>')
    return '\n'.join(out)


sections = []
for n, plan in enumerate(plans, 1):
    pipelines = len({o['pipeline'] for o in plan['operators']})
    all_steps = ''.join(f'<h4>{k}. {esc(st["name"])}</h4><pre>{esc(st["plan"])}</pre>' for k, st in enumerate(plan['steps'], 1))
    sections.append(f'''
<section>
  <h2>Query {n}</h2>
  <pre class="sql">{esc(plan['sql'])}</pre>
  <div class="plans">
    <div><h3>Calcite logical plan <span class="note">(what our optimizer gets today)</span></h3><pre>{esc(plan['logical'])}</pre></div>
    <div><h3>Physical plan, Calcite's choice <span class="note">(before Pipelines.finish: Retain and UnionAll still in place)</span></h3><pre>{esc(step(plan, 'physical operators'))}</pre></div>
    <div><h3>Physical plan, pipelined <span class="note">(after Pipelines.finish)</span></h3><pre>{esc(plan['physical'])}</pre></div>
  </div>
  <details><summary>All planner steps ({len(plan['steps'])})</summary>{all_steps}</details>
  <h3>Pipelines <span class="note">({pipelines} pipelines, {len(plan['operators'])} operators; data flows left to right)</span></h3>
  <div class="diagram">{diagram(plan['operators'])}</div>
</section>''')

page = f'''<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>Calcite physical plans</title>
<style>
  body {{ font: 15px/1.45 system-ui, sans-serif; color: #111827; margin: 24px auto; max-width: 1700px; padding: 0 16px; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  h2 {{ font-size: 18px; margin: 0 0 8px; }}
  h3 {{ font-size: 14px; margin: 14px 0 6px; color: #374151; }}
  h4 {{ font-size: 13px; margin: 12px 0 4px; color: #4b5563; }}
  .note {{ font-weight: 400; color: #6b7280; }}
  section {{ border-top: 1px solid #e5e7eb; padding: 20px 0; }}
  pre {{ font: 12px/1.4 ui-monospace, Menlo, Consolas, monospace; background: #f3f4f6; padding: 10px 12px; border-radius: 6px; overflow-x: auto; margin: 0; }}
  pre.sql {{ background: #eef2ff; white-space: pre-wrap; }}
  .plans {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; }}
  details {{ margin-top: 10px; }}
  summary {{ cursor: pointer; color: #374151; font-weight: 600; }}
  .diagram {{ overflow-x: auto; border: 1px solid #e5e7eb; border-radius: 8px; padding: 6px; }}
  svg text {{ font: 12px ui-monospace, Menlo, Consolas, monospace; fill: #111827; }}
  svg text.head {{ font-weight: 700; fill: #6b7280; }}
  svg text.schema {{ fill: #6b7280; }}
  .legend span {{ font-weight: 700; margin-right: 14px; }}
</style></head><body>
<h1>Calcite physical plans for NebulaStream</h1>
<p>Sources: <code>events</code> has one physical source (1), <code>events2</code> has two (2 and 3); all fields are UINT64.
In the diagrams, each box is one pipeline; under each operator, <code>→</code> lists its output fields (physical names, <code>?</code> = nullable).</p>
<p class="legend"><span style="color:{COLOR['start']}">starts a pipeline</span><span style="color:{COLOR['middle']}">in a pipeline</span><span style="color:{COLOR['end']}">ends a pipeline</span></p>
{''.join(sections)}
</body></html>'''
open(sys.argv[2], 'w').write(page)
print('written', sys.argv[2])
