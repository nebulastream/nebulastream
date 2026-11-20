#!/usr/bin/env python3
"""Renders the JSON of DumpSteps.java as an HTML page: per query, every planner step as a graph, and for the rule-based
steps the plan after each rule. Graphs are Graphviz DOT, drawn in the browser by viz.js.

python3 render_steps.py <steps.json> <out.html>

With --systest, for the requests of a systest run (DumpSteps.java --requests): one page per test file and an index.
python3 render_steps.py --systest <steps.json> <nes-systests directory> <out directory>

--viz <viz-standalone.js> first puts viz.js into the pages (otherwise they load it from a CDN).
"""
import html
import json
import os
import re
import sys

EXAMPLES_INTRO = """<h1>Calcite planner steps</h1>
<p>Each query is planned by <code>Planner.plan</code> with <code>detailed_steps</code>. Every step shows the plan after it,
as a graph (rows flow upwards) and as text. The rule-based steps also show the plan after each rule that changed it; the
operator that the rule produced has a red border. Topology: worker <b>localhost:8181 &rarr; localhost:8282</b> (workers
are named by their gRPC address), the sink on localhost:8282; <code>events</code> has one physical source (on
localhost:8181), <code>events2</code> two (one on each worker).</p>
"""

PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>@@TITLE@@</title>
@@VIZ@@
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
details.file { border-top: 1px solid #e5e7eb; margin-top: 1rem; }
details.file > summary { cursor: pointer; font-size: 1.2rem; font-weight: 600; padding: .6rem 0; }
.toc { columns: 3; font-size: .85rem; }
.note { color: #92400e; background: #fef3c7; padding: .4rem .7rem; border-radius: 6px; }
</style></head><body>
@@INTRO@@
<p class="legend"><span style="background:#dbeafe">logical operator</span><span style="background:#fde68a">physical operator</span>
<span style="background:#bbf7d0">window</span><span style="background:#fed7aa">network</span>
<span style="border-color:#dc2626;border-width:3px">changed by the rule</span>
<span style="border:1px dashed #888">a worker</span><span style="border-color:#2563eb">a pipeline</span></p>
<p>Who does what: <span class="badge calcite">Calcite</span> Calcite's own code or rule,
<span class="badge ours">ours</span> our code or rule (package <code>nes.calcite</code>),
<span class="badge mixed">rules</span> a rule program whose rules (Calcite's, ours, or both) we selected and ordered.</p>
@@SECTIONS@@
<script>
/// viz.js draws the graphs; without it (blocked or offline), each graph says so instead of "drawing".
const vizReady = typeof Viz === "undefined" ? Promise.reject(new Error("viz.js did not load")) : Viz.instance();
vizReady.catch(() => {});
function draw(el) {
  if (el.dataset.drawn) return;
  el.dataset.drawn = "1";
  vizReady.then(viz => { el.innerHTML = ""; el.appendChild(viz.renderSVGElement(el.dataset.dot)); })
    .catch(e => { el.textContent = "cannot draw: " + e; });
}
/// A step's graphs are drawn when it is open and visible: not inside a closed test file (the page of all test files).
function visible(d) { const file = d.closest("details.file"); return !file || file.open; }
document.querySelectorAll("details.step").forEach(d => d.addEventListener("toggle", () => {
  if (d.open && visible(d)) d.querySelectorAll(":scope > .body > .graph, :scope .rules .graph.current").forEach(draw);
}));
document.querySelectorAll("details.file").forEach(f => f.addEventListener("toggle", () => {
  if (f.open) f.querySelectorAll("details.step[open]").forEach(d => d.dispatchEvent(new Event("toggle")));
}));
document.querySelectorAll(".rules").forEach(box => {
  const items = box.querySelectorAll("li"), views = box.querySelectorAll(".rule");
  function show(i) {
    items.forEach((li, j) => li.classList.toggle("active", i === j));
    views.forEach((v, j) => {
      v.hidden = i !== j;
      const g = v.querySelector(".graph");
      g.classList.toggle("current", i === j);
      if (i === j && box.closest("details.step").open && visible(box)) draw(g); /// drawn when the step is opened
    });
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


def query_html(title, query):
    section = ['<h2 id="%s">%s</h2><code class="sql">%s</code>' % (html.escape(title.replace(" ", "-")), html.escape(title),
                                                                     html.escape(query["sql"]))]
    if query.get("logical") and not query.get("label"):
        section.append('<p class="note">Planned as a logical plan with finite inputs: its joins do not exist as physical'
                       ' operators yet, so it is neither placed nor pipelined.</p>')
    if query["error"]:
        section.append('<p class="error">%s</p>' % html.escape(query["error"]))
    previous = None
    physical = query.get("physical")
    for i, step in enumerate(query["steps"]):
        section.append(step_html(step, previous, i == len(query["steps"]) - 1 and not (physical and physical["steps"])))
        previous = step["plan"]
    if physical:
        previous = None
        section.append('<h3>Physical path: placement and pipelines</h3><p class="origin">The same query planned down to placed'
                       ' pipelines (the physical path, PlanRequest.physical), all its steps: the sources are expanded first.'
                       ' The step &ldquo;physical operators&rdquo; lists each conversion of a logical operator, with its rule and'
                       ' the reason. Workers: one per host of a physical source (%s), all sending to the first, which runs'
                       ' the sink.</p>' % html.escape(physical["hosts"]))
        if physical["error"]:
            section.append('<p class="note">The physical path cannot plan this query yet: %s</p>' % html.escape(physical["error"]))
        for i, step in enumerate(physical["steps"]):
            section.append(step_html(step, previous, i == len(physical["steps"]) - 1))
            previous = step["plan"]
    return "".join(section)


VIZ_CDN = '<script src="https://cdn.jsdelivr.net/npm/@viz-js/viz@3.11.0/lib/viz-standalone.js"></script>'
viz_script = VIZ_CDN


def page(title, intro, sections):
    return (PAGE.replace("@@VIZ@@", viz_script).replace("@@TITLE@@", html.escape(title)).replace("@@INTRO@@", intro)
            .replace("@@SECTIONS@@", "\n".join(sections)))


def examples(steps_json, out_html):
    queries = json.load(open(steps_json))
    sections = [query_html("Query %d" % n, query) for n, query in enumerate(queries, 1)]
    with open(out_html, "w") as out:
        out.write(page("Calcite planner steps", EXAMPLES_INTRO, sections))


def file_key(relative):
    """systest's key of a test file (TestFileKeyFactory in NamePrefixer.cpp): upper case, a letter or digit as it is, `__` an
    underscore, `_D_` a directory separator, `_<hex>_` any other character, and a leading digit."""
    key = []
    for i, c in enumerate(relative.upper()):
        if c.isascii() and (c.isalpha() or (c.isdigit() and i > 0)):
            key.append(c)
        elif c == "_":
            key.append("__")
        elif c == "/":
            key.append("_D_")
        else:
            key.append("_%02X_" % ord(c))
    return "".join(key)


def test_files(tests_root):
    """systest's key of a test file (e.g. INFERENCE_D_INFERMODEL) -> its path relative to tests_root."""
    files = {}
    for directory, _, names in os.walk(tests_root):
        for name in names:
            if name.endswith(".test"):
                relative = os.path.relpath(os.path.join(directory, name), tests_root)[:-len(".test")]
                files[file_key(relative)] = relative
    return files


def test_file_of(key, files):
    """The test file of a key and the part of it (systest splits a file into parts, `<key>_C<part>`), if any."""
    if key in files:
        return files[key], None
    match = re.fullmatch(r"(.*)_C(\d+)", key)
    if match and match.group(1) in files:
        return files[match.group(1)], int(match.group(2))
    return key, None


def query_number(label):
    """1, 2, ... of `KEY:2` or `KEY:2-differential`, for sorting."""
    match = re.search(r":(\d+)", label)
    return (int(match.group(1)) if match else 0, label)


def systest(steps_json, tests_root, out_dir):
    queries = json.load(open(steps_json))
    files = test_files(tests_root)
    by_file = {}
    for query in queries:
        key, _, number = query["label"].partition(":")
        relative, part = test_file_of(key, files)
        query["number"] = number if part is None else "%s (part %d of the file)" % (number, part + 1)
        by_file.setdefault(relative, []).append(query)
    os.makedirs(out_dir, exist_ok=True)
    rows = []
    for relative in sorted(by_file):
        file_queries = sorted(by_file[relative], key=lambda q: query_number(q["label"]))
        page_name = relative.replace("/", "__") + ".html"
        test_path = os.path.abspath(os.path.join(tests_root, relative + ".test"))
        intro = ('<p><a href="index.html">&larr; all test files</a></p><h1>%s</h1><p>The planner\'s steps for every query of '
                 '<a href="file://%s">%s</a>, planned from the request that systest sent (logical plan, finite inputs).</p>'
                 % (html.escape(relative), html.escape(test_path), html.escape(test_path)))
        sections = [query_html("Query %s" % q["number"], q) for q in file_queries]
        with open(os.path.join(out_dir, page_name), "w") as out:
            out.write(page(relative, intro, sections))
        errors = sum(1 for q in file_queries if q["error"])
        rows.append('<tr><td><a href="%s">%s</a></td><td>%d</td><td>%s</td></tr>' % (
            html.escape(page_name), html.escape(relative), len(file_queries), errors or ""))
    index = ("<!doctype html><html><head><meta charset=\"utf-8\"><title>Planner steps of the systests</title><style>"
             "body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 2rem auto; max-width: 900px; }"
             "td, th { padding: .2rem .8rem; text-align: left; } tr:nth-child(even) { background: #f3f4f6; }</style></head><body>"
             "<h1>Planner steps of the systests</h1><p>%d queries in %d test files. A page shows every planner step of each"
             " query of one file. Planner errors: the queries whose planning failed, mostly tests that expect an error.</p><table><tr><th>test file</th>"
             "<th>queries</th><th>planner errors</th></tr>%s</table></body></html>" % (len(queries), len(rows), "".join(rows)))
    with open(os.path.join(out_dir, "index.html"), "w") as out:
        out.write(index)

    # Everything in one page: a section per test file, closed until opened.
    toc, files_html = [], []
    for relative in sorted(by_file):
        file_queries = sorted(by_file[relative], key=lambda q: query_number(q["label"]))
        anchor = "file-" + relative.replace("/", "--")
        errors = sum(1 for q in file_queries if q["error"])
        toc.append('<li><a href="#%s" onclick="document.getElementById(\'%s\').open = true">%s</a> (%d)</li>' % (html.escape(anchor), html.escape(anchor), html.escape(relative), len(file_queries)))
        sections = [query_html("%s, query %s" % (relative, q["number"]), q) for q in file_queries]
        files_html.append('<details class="file" id="%s"><summary>%s <span class="meta">%d queries%s</span></summary>%s</details>' % (
            html.escape(anchor), html.escape(relative), len(file_queries), ", %d planner errors" % errors if errors else "",
            "".join(sections)))
    intro = ('<h1>Planner steps of the systests</h1><p>%d queries in %d test files, each planned from the request that systest'
             ' sent (logical plan, finite inputs). Open a test file, then a step; its graph is drawn when opened. Planner'
             ' errors are mostly tests that expect an error.</p><details><summary>Jump to a test file</summary><ul class="toc">%s</ul>'
             '</details>' % (len(queries), len(toc), "".join(toc)))
    with open(os.path.join(out_dir, "all.html"), "w") as out:
        out.write(page("Planner steps of the systests", intro, files_html))


def main():
    global viz_script
    if sys.argv[1] == "--viz":  # viz.js in the page, so that it draws without the CDN
        viz_script = "<script>%s</script>" % open(sys.argv[2]).read()
        del sys.argv[1:3]
    if sys.argv[1] == "--systest":
        systest(sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        examples(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()
