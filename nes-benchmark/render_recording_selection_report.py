#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.patches import Circle
except ModuleNotFoundError:
    print("render_recording_selection_report.py requires matplotlib", file=sys.stderr)
    raise SystemExit(1)

COLOR_SOLVER = "#1b9e77"
COLOR_BASELINE = "#4d4d4d"
COLOR_GRAPH = "#9aa0a6"
COLOR_LABEL_BG = "#fffdf6"
COLOR_NODE_DEFAULT = "#f4f1ea"
COLOR_NODE_SOURCE = "#dae8fc"
COLOR_NODE_STATEFUL = "#ffe5d2"
COLOR_NODE_SINK = "#e2e3e5"
COLOR_HOST_FILL = "#f8f9fa"
COLOR_HOST_EDGE = "#495057"
COLOR_PLACEMENT_GUIDE = "#c9ced6"
NODE_COLORS = {
    "source": COLOR_NODE_SOURCE,
    "sink": COLOR_NODE_SINK,
    "aggregate-keyed": COLOR_NODE_STATEFUL,
    "aggregate-window": COLOR_NODE_STATEFUL,
    "join-hash": COLOR_NODE_STATEFUL,
    "join-window": COLOR_NODE_STATEFUL,
}
SCENARIO_LABELS = {
    "kary-tree-recording-selection": "k-ary tree",
    "dag-diamond-recording-selection": "diamond",
    "dag-join-recording-selection": "join",
    "dag-pipeline-recording-selection": "pipeline",
    "dag-union-recording-selection": "union",
    "dag-multi-join-recording-selection": "multi-join",
    "dag-join-union-hybrid-recording-selection": "join-union-hybrid",
    "dag-shared-subplan-recording-selection": "shared-subplan",
    "dag-branching-pipeline-recording-selection": "branching-pipeline",
    "dag-reconverging-fanout-recording-selection": "reconverging-fanout",
}
KIND_LABELS = {
    "source": "src",
    "sink": "sink",
    "filter": "flt",
    "map": "map",
    "project": "prj",
    "enrich": "enr",
    "union": "uni",
    "aggregate-keyed": "aggk",
    "aggregate-window": "aggw",
    "join-hash": "jnh",
    "join-window": "jnw",
    "tree": "tree",
}
DECISION_LABELS = {
    "create": "C",
    "upgrade": "U",
    "reuse": "R",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a paginated recording-selection report PDF from benchmark report JSON.")
    parser.add_argument("input_json", type=Path, help="Report JSON emitted by recording-selection-benchmark --report-output")
    parser.add_argument("output_path", type=Path, help="Output PDF path")
    parser.add_argument("--columns", type=int, default=3, help="Grid columns per page")
    parser.add_argument("--rows", type=int, default=3, help="Grid rows per page")
    parser.add_argument("--page-width", type=float, default=14.0, help="Page width in inches")
    parser.add_argument("--page-height", type=float, default=10.0, help="Page height in inches")
    parser.add_argument(
        "--sort-by",
        choices=("input", "scenario", "bucket"),
        default="input",
        help="Instance ordering inside the report")
    parser.add_argument("--title", default="Recording Selection Report", help="Document title")
    return parser.parse_args()


def scenario_label(raw_name: str) -> str:
    return SCENARIO_LABELS.get(raw_name, raw_name.replace("-recording-selection", "").replace("-", " "))


def node_color(kind: str) -> str:
    return NODE_COLORS.get(kind, COLOR_NODE_DEFAULT)


def node_label(kind: str) -> str:
    return KIND_LABELS.get(kind, kind[:4])


def normalize_positions(raw_positions: dict[int, tuple[float, float]]) -> dict[int, tuple[float, float]]:
    xs = [point[0] for point in raw_positions.values()]
    ys = [point[1] for point in raw_positions.values()]
    min_x, max_x = min(xs), max(xs)
    min_y, max_y = min(ys), max(ys)
    span_x = max(max_x - min_x, 1e-9)
    span_y = max(max_y - min_y, 1e-9)
    return {
        index: ((point[0] - min_x) / span_x, (point[1] - min_y) / span_y)
        for index, point in raw_positions.items()
    }


def topology_positions(instance: dict) -> dict[int, tuple[float, float]]:
    host_count = len(instance["hosts"])
    topology = instance["network_topology"]
    if host_count == 1:
        return {0: (0.5, 0.5)}

    raw: dict[int, tuple[float, float]] = {}
    if topology == "line":
        for index in range(host_count):
            raw[index] = (float(index), 0.0)
    elif topology in {"ring", "random-connected"}:
        for index in range(host_count):
            angle = (2.0 * math.pi * index) / host_count
            raw[index] = (math.cos(angle), math.sin(angle))
    elif topology == "star":
        raw[0] = (0.0, 0.0)
        outer_count = max(1, host_count - 1)
        for index in range(1, host_count):
            angle = (2.0 * math.pi * (index - 1)) / outer_count
            raw[index] = (1.2 * math.cos(angle), 1.2 * math.sin(angle))
    elif topology == "tree":
        for index in range(host_count):
            level = int(math.floor(math.log2(index + 1)))
            first_at_level = (1 << level) - 1
            position_in_level = index - first_at_level
            raw[index] = (position_in_level - ((1 << level) - 1) / 2.0, -float(level))
    else:
        for index in range(host_count):
            raw[index] = (float(index), 0.0)

    return normalize_positions(raw)


def dag_depths(instance: dict) -> dict[int, int]:
    nodes = instance["nodes"]
    edges = instance["edges"]
    if not nodes:
        return {}

    children_by_parent: dict[int, list[int]] = {}
    indegree = {node["index"]: 0 for node in nodes}
    for edge in edges:
        children_by_parent.setdefault(edge["parent_node_index"], []).append(edge["child_node_index"])
        indegree[edge["child_node_index"]] = indegree.get(edge["child_node_index"], 0) + 1

    roots = [node["index"] for node in nodes if indegree.get(node["index"], 0) == 0]
    if not roots:
        roots = [nodes[0]["index"]]

    depth = {root: 0 for root in roots}
    queue = list(roots)
    for current in queue:
        current_depth = depth[current]
        for child in children_by_parent.get(current, []):
            next_depth = current_depth + 1
            if child not in depth or next_depth > depth[child]:
                depth[child] = next_depth
            if child not in queue:
                queue.append(child)

    for node in nodes:
        depth.setdefault(node["index"], 0)
    return depth


def node_host_assignments(instance: dict) -> dict[int, int]:
    votes: dict[int, list[int]] = {node["index"]: [] for node in instance["nodes"]}
    for edge in instance["edges"]:
        route = edge["route_host_indices"]
        if not route:
            continue
        votes[edge["child_node_index"]].append(route[0])
        votes[edge["parent_node_index"]].append(route[-1])

    assignments: dict[int, int] = {}
    for node in instance["nodes"]:
        node_index = node["index"]
        host_votes = votes.get(node_index, [])
        if not host_votes:
            assignments[node_index] = 0
            continue
        counts: dict[int, int] = {}
        for host_index in host_votes:
            counts[host_index] = counts.get(host_index, 0) + 1
        assignments[node_index] = min(counts.items(), key=lambda item: (-item[1], item[0]))[0]
    return assignments


def integrated_host_positions(instance: dict) -> dict[int, tuple[float, float]]:
    base_positions = topology_positions(instance)
    return {
        host_index: (0.08 + 0.84 * x_coord, 0.08 + 0.14 * y_coord)
        for host_index, (x_coord, y_coord) in base_positions.items()
    }


def integrated_node_positions(instance: dict) -> dict[int, tuple[float, float]]:
    depths = dag_depths(instance)
    assignments = node_host_assignments(instance)
    host_positions = integrated_host_positions(instance)
    max_depth = max(depths.values(), default=0)

    grouped: dict[tuple[int, int], list[int]] = {}
    for node in sorted(instance["nodes"], key=lambda item: (depths[item["index"]], item["index"])):
        key = (assignments[node["index"]], depths[node["index"]])
        grouped.setdefault(key, []).append(node["index"])

    positions: dict[int, tuple[float, float]] = {}
    for (host_index, depth), node_indices in grouped.items():
        host_x, _ = host_positions[host_index]
        if max_depth == 0:
            base_y = 0.62
        else:
            base_y = 0.30 + 0.58 * (1.0 - (depth / max_depth))
        count = len(node_indices)
        x_step = 0.032 if count <= 3 else 0.024
        for offset_index, node_index in enumerate(node_indices):
            x_offset = (offset_index - (count - 1) / 2.0) * x_step
            y_offset = 0.018 * ((offset_index % 2) - 0.5) if count > 1 else 0.0
            positions[node_index] = (host_x + x_offset, base_y + y_offset)
    return positions


def dag_positions(instance: dict) -> dict[int, tuple[float, float]]:
    nodes = instance["nodes"]
    if not nodes:
        return {}

    depth = dag_depths(instance)

    layers: dict[int, list[int]] = {}
    for node_index, node_depth in depth.items():
        layers.setdefault(node_depth, []).append(node_index)

    max_depth = max(layers)
    raw: dict[int, tuple[float, float]] = {}
    for layer_depth, node_indices in sorted(layers.items()):
        sorted_indices = sorted(node_indices)
        count = len(sorted_indices)
        for position, node_index in enumerate(sorted_indices):
            y = 0.5 if count == 1 else position / (count - 1)
            x = 0.0 if max_depth == 0 else layer_depth / max_depth
            raw[node_index] = (x, 1.0 - y)

    return raw


def boundary_hosts(instance: dict, boundary_entries: list[dict]) -> set[int]:
    edges_by_index = {edge["index"]: edge for edge in instance["edges"]}
    hosts: set[int] = set()
    for entry in boundary_entries:
        hosts.add(entry["selection_host_index"])
        route = edges_by_index[entry["edge_index"]]["route_host_indices"]
        if route:
            hosts.add(route[0])
            hosts.add(route[-1])
    return hosts


def draw_integrated_graph(ax: plt.Axes, instance: dict, boundary_entries: list[dict] | None, title: str, highlight_color: str) -> None:
    host_positions = integrated_host_positions(instance)
    node_positions = integrated_node_positions(instance)
    selected_hosts = boundary_hosts(instance, boundary_entries or [])
    highlighted = {entry["edge_index"]: entry for entry in (boundary_entries or [])}
    node_hosts = node_host_assignments(instance)

    for link in instance["host_links"]:
        lhs = link["from"]
        rhs = link["to"]
        color = "#d4d8de"
        width = 1.0
        x_values = [host_positions[lhs][0], host_positions[rhs][0]]
        y_values = [host_positions[lhs][1], host_positions[rhs][1]]
        ax.plot(x_values, y_values, color=color, linewidth=width, solid_capstyle="round", alpha=0.75, zorder=1)

    for host in instance["hosts"]:
        host_index = host["index"]
        x_coord, y_coord = host_positions[host_index]
        edge_color = highlight_color if host_index in selected_hosts else COLOR_HOST_EDGE
        edge_width = 1.3 if host_index in selected_hosts else 1.0
        ax.add_patch(Circle((x_coord, y_coord), radius=0.040, facecolor=COLOR_HOST_FILL, edgecolor=edge_color, linewidth=edge_width, zorder=2))
        ax.text(x_coord, y_coord, f"w{host_index}", fontsize=5.6, ha="center", va="center", zorder=3)

    for node in instance["nodes"]:
        node_index = node["index"]
        host_index = node_hosts[node_index]
        x_values = [host_positions[host_index][0], node_positions[node_index][0]]
        y_values = [host_positions[host_index][1], node_positions[node_index][1]]
        ax.plot(
            x_values,
            y_values,
            color=COLOR_PLACEMENT_GUIDE,
            linewidth=0.7,
            linestyle=(0, (1.6, 1.8)),
            alpha=0.9,
            zorder=2)

    for edge in instance["edges"]:
        parent = edge["parent_node_index"]
        child = edge["child_node_index"]
        x_values = [node_positions[parent][0], node_positions[child][0]]
        y_values = [node_positions[parent][1], node_positions[child][1]]
        if edge["index"] in highlighted:
            color = highlight_color
            width = 2.8
            zorder = 4
        else:
            color = COLOR_GRAPH
            width = 0.65
            zorder = 3
        ax.plot(x_values, y_values, color=color, linewidth=width, solid_capstyle="round", alpha=0.95, zorder=zorder)

    for edge_index, selection in highlighted.items():
        edge = next(edge for edge in instance["edges"] if edge["index"] == edge_index)
        parent = edge["parent_node_index"]
        child = edge["child_node_index"]
        x_coord = (node_positions[parent][0] + node_positions[child][0]) / 2.0
        y_coord = (node_positions[parent][1] + node_positions[child][1]) / 2.0
        decision = DECISION_LABELS.get(selection["decision"], selection["decision"][:1].upper())
        label = f"{decision}@w{selection['selection_host_index']}"
        ax.text(
            x_coord,
            y_coord,
            label,
            fontsize=4.9,
            ha="center",
            va="center",
            bbox={"boxstyle": "round,pad=0.12", "facecolor": COLOR_LABEL_BG, "edgecolor": "none", "alpha": 0.92},
            zorder=3)

    for node in instance["nodes"]:
        node_index = node["index"]
        x_coord, y_coord = node_positions[node_index]
        ax.add_patch(
            Circle(
                (x_coord, y_coord),
                radius=0.038,
                facecolor=node_color(node["kind"]),
                edgecolor="#333333",
                linewidth=0.8,
                zorder=5))
        ax.text(x_coord, y_coord, node_label(node["kind"]), fontsize=5.0, ha="center", va="center", zorder=6)

    ax.set_title(title, fontsize=7.0, pad=1.5)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.axis("off")


def draw_header(ax: plt.Axes, instance: dict) -> None:
    ax.axis("off")
    title = f"{scenario_label(instance['scenario'])} | {instance['corpus_bucket']} | {instance['instance_id']}"
    line_two = (
        f"seed {instance['seed']} | size {instance['fanout']} | hosts {instance['host_count']} | "
        f"nodes {instance['node_count']} | bind {'yes' if instance['constraints_bind'] else 'no'}"
    )
    solver = instance["solver_metrics"]
    if "exact_baseline_metrics" in instance:
        baseline = instance["exact_baseline_metrics"]
        line_three = (
            f"solver {solver['maintenance_cost']:.1f} / {solver['replay_time_ms']:.1f}ms | "
            f"baseline {baseline['maintenance_cost']:.1f} / {baseline['replay_time_ms']:.1f}ms | "
            f"rank {instance['selected_rank']}"
        )
    else:
        line_three = (
            f"solver {solver['maintenance_cost']:.1f} / {solver['replay_time_ms']:.1f}ms | "
            f"budget {instance['replay_latency_limit_ms']}ms | selected edges {solver['selected_edge_count']}"
        )

    ax.text(0.0, 0.95, title, fontsize=7.0, fontweight="bold", ha="left", va="top")
    ax.text(0.0, 0.48, line_two, fontsize=5.8, ha="left", va="top")
    ax.text(0.0, 0.04, line_three, fontsize=5.8, ha="left", va="bottom")


def render_instance_cell(fig: plt.Figure, cell_spec, instance: dict) -> None:
    has_baseline = "exact_baseline_boundary" in instance
    if has_baseline:
        inner = cell_spec.subgridspec(2, 2, height_ratios=[0.30, 1.0], hspace=0.18, wspace=0.16)
    else:
        inner = cell_spec.subgridspec(2, 1, height_ratios=[0.30, 1.0], hspace=0.18)

    header_ax = fig.add_subplot(inner[0, :] if has_baseline else inner[0, 0])
    draw_header(header_ax, instance)

    solver_ax = fig.add_subplot(inner[1, 0] if has_baseline else inner[1, 0])
    draw_integrated_graph(solver_ax, instance, instance.get("solver_boundary", []), "Solver on Topology", COLOR_SOLVER)

    if has_baseline:
        baseline_ax = fig.add_subplot(inner[1, 1])
        draw_integrated_graph(
            baseline_ax,
            instance,
            instance.get("exact_baseline_boundary", []),
            "Baseline on Topology",
            COLOR_BASELINE)


def sort_instances(instances: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "input":
        return instances
    if sort_by == "scenario":
        return sorted(instances, key=lambda item: (item["scenario"], item["fanout"], item["seed"], item["instance_id"]))
    return sorted(instances, key=lambda item: (item["corpus_bucket"], item["scenario"], item["fanout"], item["seed"], item["instance_id"]))


def main() -> int:
    args = parse_args()
    if args.columns <= 0 or args.rows <= 0:
        print("--columns and --rows must be positive", file=sys.stderr)
        return 1

    root = json.loads(args.input_json.read_text())
    instances = sort_instances(root.get("instances", []), args.sort_by)
    if not instances:
        print("report JSON does not contain any instances", file=sys.stderr)
        return 1

    per_page = args.columns * args.rows
    page_count = math.ceil(len(instances) / per_page)

    with PdfPages(args.output_path) as pdf:
        for page_index in range(page_count):
            page_instances = instances[page_index * per_page : (page_index + 1) * per_page]
            fig = plt.figure(figsize=(args.page_width, args.page_height))
            grid = fig.add_gridspec(
                args.rows,
                args.columns,
                left=0.03,
                right=0.97,
                bottom=0.03,
                top=0.94,
                hspace=0.18,
                wspace=0.10)

            exact_count = sum(1 for instance in page_instances if "exact_baseline_boundary" in instance)
            performance_count = len(page_instances) - exact_count
            fig.suptitle(
                f"{args.title}  |  page {page_index + 1}/{page_count}  |  "
                f"instances {len(page_instances)}  |  exact {exact_count}  |  performance {performance_count}",
                fontsize=10.5,
                x=0.5,
                y=0.975)

            for cell_index, instance in enumerate(page_instances):
                render_instance_cell(fig, grid[cell_index // args.columns, cell_index % args.columns], instance)

            pdf.savefig(fig)
            plt.close(fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
