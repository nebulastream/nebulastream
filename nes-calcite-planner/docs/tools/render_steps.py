#!/usr/bin/env python3
"""Renders the JSON of DumpSteps.java as an HTML page: per query, every planner step as a graph, and for the rule-based
steps the plan after each rule. Graphs are Graphviz DOT, drawn in the browser by viz.js.

python3 render_steps.py <steps.json> <out.html>
"""
import html
import json
import sys

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>Calcite planner steps</title>
<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.11.0/lib/viz-standalone.js"></script>
<style>
body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 2rem auto; max-width: 1200px; color: #1f2937; }
h1 { font-size: 1.6rem; } h2 { font-size: 1.2rem; margin-top: 2.5rem; border-top: 1px solid #e5e7eb; padding-top: 1.2rem; }
code.sql { display: block; background: #f3f4f6; padding: .7rem; border-radius: 6px; white-space: pre-wrap; }
details.step { border: 1px solid #e5e7eb; border-radius: 8px; margin: .5rem 0; background: #fff; }
details.step > summary { cursor: pointer; padding: .5rem .8rem; font-weight: 600; }
details.step > summary .meta { font-weight: normal; color: #6b7280; margin-left: .5rem; }
details.step.same > summary { color: #9ca3af; }
.body { padding: .3rem .8rem .8rem; }
.graph { overflow-x: auto; text-align: center; }
.graph svg { max-width: 100%; height: auto; }
pre { background: #f9fafb; padding: .6rem; border-radius: 6px; overflow-x: auto; font-size: .8rem; }
.rules { display: flex; gap: 1rem; }
.rules ol { min-width: 15rem; max-width: 18rem; margin: 0; padding-left: 1.5rem; font-size: .85rem; }
.rules li { cursor: pointer; padding: .15rem .3rem; border-radius: 4px; }
.rules li.active { background: #fee2e2; font-weight: 600; }
.rules .view { flex: 1; min-width: 0; }
.nav button { margin-right: .3rem; }
.legend span { display: inline-block; padding: .1rem .5rem; border-radius: 4px; margin-right: .4rem; border: 1px solid #9ca3af; font-size: .85rem; }
.error { color: #b91c1c; white-space: pre-wrap; }
.badge { display: inline-block; font-size: .7rem; font-weight: 600; padding: 0 .35rem; border-radius: 4px; margin-right: .35rem; vertical-align: middle; }
.badge.calcite { background: #e0e7ff; color: #3730a3; border: 1px solid #a5b4fc; }
.badge.ours { background: #dcfce7; color: #166534; border: 1px solid #86efac; }
.badge.mixed { background: #fef9c3; color: #854d0e; border: 1px solid #fde047; }
.origin { color: #4b5563; font-size: .85rem; margin: .2rem 0 .5rem; }
.note { color: #92400e; background: #fef3c7; padding: .4rem .7rem; border-radius: 6px; }
</style></head><body>
<h1>Calcite planner steps</h1>
<p>Each query is planned by <code>Planner.plan</code> with <code>detailed_steps</code>. Every step shows the plan after it,
as a graph (rows flow upwards) and as text. The rule-based steps also show the plan after each rule that changed it; the
operator that the rule produced has a red border. Topology: worker <b>localhost:8181 &rarr; localhost:8282</b> (workers
are named by their gRPC address), the sink on localhost:8282; <code>events</code> has one physical source (on
localhost:8181), <code>events2</code> two (one on each worker).</p>
<p class="legend"><span style="background:#dbeafe">logical operator</span><span style="background:#fde68a">physical operator</span>
<span style="background:#bbf7d0">window</span><span style="background:#fed7aa">network</span>
<span style="border-color:#dc2626;border-width:3px">changed by the rule</span>
<span style="border:1px dashed #888">a worker</span><span style="border-color:#2563eb">a pipeline</span></p>
<p>Who does what: <span class="badge calcite">Calcite</span> Calcite's own code or rule,
<span class="badge ours">ours</span> our code or rule (package <code>nes.calcite</code>),
<span class="badge mixed">rules</span> a rule program whose rules (Calcite's, ours, or both) we selected and ordered.</p>
@@SECTIONS@@
<script>
const vizReady = Viz.instance();
function draw(el) {
  if (el.dataset.drawn) return;
  el.dataset.drawn = "1";
  vizReady.then(viz => { el.innerHTML = ""; el.appendChild(viz.renderSVGElement(el.dataset.dot)); })
    .catch(e => { el.textContent = "cannot draw: " + e; });
}
document.querySelectorAll("details.step").forEach(d => d.addEventListener("toggle", () => {
  if (d.open) d.querySelectorAll(":scope > .body > .graph, :scope .rules .graph.current").forEach(draw);
}));
document.querySelectorAll(".rules").forEach(box => {
  const items = box.querySelectorAll("li"), views = box.querySelectorAll(".rule");
  function show(i) {
    items.forEach((li, j) => li.classList.toggle("active", i === j));
    views.forEach((v, j) => { v.hidden = i !== j; const g = v.querySelector(".graph"); g.classList.toggle("current", i === j); if (i === j) draw(g); });
    box.dataset.index = i;
  }
  items.forEach((li, i) => li.addEventListener("click", () => show(i)));
  box.querySelector(".prev").addEventListener("click", () => show(Math.max(0, +box.dataset.index - 1)));
  box.querySelector(".next").addEventListener("click", () => show(Math.min(items.length - 1, +box.dataset.index + 1)));
  show(0);
});
document.querySelectorAll("details.step[open]").forEach(d => d.dispatchEvent(new Event("toggle")));
</script>
</body></html>
"""


def graph(dot):
    return '<div class="graph" data-dot="%s">drawing &hellip;</div>' % html.escape(dot, quote=True)


def origin_badge(origin):
    kind = origin.split(":")[0]
    css = {"Calcite": "calcite", "ours": "ours"}.get(kind, "mixed")
    return '<span class="badge %s">%s</span>' % (css, html.escape(kind))


def rule_badge(rule):
    return '<span class="badge %s">%s</span>' % (("ours", "ours") if rule.get("ours") else ("calcite", "Calcite"))


def step_html(step, previous_plan, last):
    rules = step["rules"]
    same = step["plan"] == previous_plan
    meta = []
    if rules:
        meta.append("%d rule%s applied" % (len(rules), "" if len(rules) == 1 else "s"))
    if same:
        meta.append("no change")
    origin = step.get("origin", "")
    parts = ['<details class="step%s"%s><summary>%s%s<span class="meta">%s</span></summary><div class="body">' % (
        " same" if same else "", " open" if last else "", origin_badge(origin) if origin else "", html.escape(step["name"]),
        html.escape(", ".join(meta)))]
    if origin:
        parts.append('<p class="origin">%s</p>' % html.escape(origin))
    parts.append(graph(step["dot"]))
    parts.append("<details><summary>text</summary><pre>%s</pre></details>" % html.escape(step["plan"]))
    if rules:
        parts.append('<h4>Rule by rule</h4><div class="rules" data-index="0"><ol>')
        parts.extend("<li>%s%s</li>" % (rule_badge(rule), html.escape(rule["rule"])) for rule in rules)
        parts.append('</ol><div class="view"><div class="nav"><button class="prev">&larr; previous rule</button>'
                     '<button class="next">next rule &rarr;</button></div>')
        for i, rule in enumerate(rules):
            parts.append('<div class="rule"%s><p><b>%d. %s%s</b></p>%s<details><summary>text</summary><pre>%s</pre></details></div>' % (
                "" if i == 0 else " hidden", i + 1, rule_badge(rule), html.escape(rule["rule"]), graph(rule["dot"]), html.escape(rule["plan"])))
        parts.append("</div></div>")
    parts.append("</div></details>")
    return "".join(parts)


def main():
    queries = json.load(open(sys.argv[1]))
    sections = []
    for n, query in enumerate(queries, 1):
        section = ['<h2>Query %d</h2><code class="sql">%s</code>' % (n, html.escape(query["sql"]))]
        if query.get("logical"):
            section.append('<p class="note">Planned as a logical plan with finite inputs: its joins do not exist as physical'
                           ' operators yet, so it is neither placed nor pipelined.</p>')
        if query["error"]:
            section.append('<p class="error">%s</p>' % html.escape(query["error"]))
        previous = None
        for i, step in enumerate(query["steps"]):
            section.append(step_html(step, previous, i == len(query["steps"]) - 1))
            previous = step["plan"]
        sections.append("".join(section))
    with open(sys.argv[2], "w") as out:
        out.write(PAGE.replace("@@SECTIONS@@", "\n".join(sections)))


if __name__ == "__main__":
    main()
