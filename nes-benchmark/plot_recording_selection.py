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
import csv
import math
import os
import random
import statistics
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

os.environ.setdefault("MPLCONFIGDIR", os.path.join(tempfile.gettempdir(), "matplotlib"))

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ModuleNotFoundError:
    print("plot_recording_selection.py requires matplotlib", file=sys.stderr)
    raise SystemExit(1)

COLOR_SELECTED = "#1b9e77"
COLOR_AVERAGE = "#d95f02"
COLOR_WORST = "#7570b3"
COLOR_BASELINE = "#4d4d4d"
CATEGORY_COLORS = ["#1b9e77", "#1f78b4", "#d95f02", "#7570b3", "#e7298a", "#66a61e"]

TOPOLOGY_LABELS = {
    "kary-tree-recording-selection": "k-ary tree",
    "dag-diamond-recording-selection": "diamond",
    "dag-join-recording-selection": "join",
    "dag-pipeline-recording-selection": "pipeline",
    "dag-union-recording-selection": "union",
}

SCATTER_BUCKETS: list[tuple[str, str, str]] = [
    ("tight", "tight\n1-5%", "#1b9e77"),
    ("medium", "medium\n5-20%", "#1f78b4"),
    ("loose", "loose\n20-50%", "#d95f02"),
    ("wide", "wide\n>50%", "#7570b3"),
]

JITTER_WIDTH = 0.18
POINT_ALPHA = 0.68
POINT_SIZE = 22.0
RNG_SEED = 42


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render paper, scatter, or shadow-price recording-selection figures from benchmark CSV output.")
    parser.add_argument("input_csv", type=Path, help="CSV emitted by recording-selection-benchmark")
    parser.add_argument("output_path", type=Path, help="Output figure path, e.g. figure.pdf")
    parser.add_argument("--width", type=float, default=6.8, help="Figure width in inches")
    parser.add_argument("--height", type=float, default=2.55, help="Figure height in inches")
    parser.add_argument(
        "--runtime-unit",
        choices=("ns", "us", "ms"),
        default="us",
        help="Unit used in the runtime subplot")
    parser.add_argument(
        "--plot-kind",
        choices=("paper", "scatter", "shadow-price-heatmap", "shadow-price-convergence"),
        default="paper",
        help="Figure style to render")
    parser.add_argument(
        "--include-nonbinding",
        action="store_true",
        help="Include non-binding scenarios in the shadow-price heatmap")
    parser.add_argument(
        "--group-by",
        choices=("feasible-cut-count", "scenario"),
        default="feasible-cut-count",
        help="Aggregate the paper/scatter plots by feasible-cut count or scenario family")
    parser.add_argument(
        "--baseline-mode",
        choices=("auto", "exact", "performance-only", "all"),
        default="auto",
        help="Filter rows by baseline mode; auto keeps exact-only for cost plots and all rows for convergence")
    return parser.parse_args()


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = fraction * (len(ordered) - 1)
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def scenario_label(raw_name: str) -> str:
    return TOPOLOGY_LABELS.get(raw_name, raw_name.replace("-recording-selection", "").replace("-", " "))


def scenario_sort_key(raw_name: str) -> tuple[int, str]:
    ordered_names = list(TOPOLOGY_LABELS)
    return (ordered_names.index(raw_name) if raw_name in ordered_names else len(ordered_names), scenario_label(raw_name))


def row_baseline_mode(row: dict[str, str]) -> str:
    return row.get("baseline_mode", "exact")


def effective_baseline_mode(plot_kind: str, requested_mode: str) -> str:
    if requested_mode != "auto":
        return requested_mode
    return "all" if plot_kind == "shadow-price-convergence" else "exact"


def select_rows_for_plot(
    rows: list[dict[str, str]],
    *,
    plot_kind: str,
    requested_mode: str,
    requires_exact_baseline: bool,
) -> list[dict[str, str]]:
    resolved_mode = effective_baseline_mode(plot_kind, requested_mode)
    if requires_exact_baseline:
        return [row for row in rows if row_baseline_mode(row) == "exact"]
    if resolved_mode == "all":
        return rows
    return [row for row in rows if row_baseline_mode(row) == resolved_mode]


def finite_float(row: dict[str, str], key: str) -> float | None:
    try:
        value = float(row[key])
    except (KeyError, ValueError):
        return None
    return value if math.isfinite(value) else None


def summarize(
    rows: list[dict[str, str]],
    key: str,
    group_key: str = "feasible_cut_count",
) -> tuple[list[int], list[float], list[float], list[float]]:
    grouped: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        value = finite_float(row, key)
        if value is None:
            continue
        grouped[int(row[group_key])].append(value)

    if not grouped:
        return [], [], [], []

    x_values = sorted(grouped)
    medians = [statistics.median(grouped[x]) for x in x_values]
    lower = [percentile(grouped[x], 0.25) for x in x_values]
    upper = [percentile(grouped[x], 0.75) for x in x_values]
    return x_values, medians, lower, upper


def summarize_categorical(
    rows: list[dict[str, str]],
    key: str,
    group_key: str = "scenario",
) -> tuple[list[str], list[float], list[float], list[float]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        value = finite_float(row, key)
        if value is None:
            continue
        grouped[row[group_key]].append(value)

    if not grouped:
        return [], [], [], []

    categories = sorted(grouped, key=scenario_sort_key)
    medians = [statistics.median(grouped[category]) for category in categories]
    lower = [percentile(grouped[category], 0.25) for category in categories]
    upper = [percentile(grouped[category], 0.75) for category in categories]
    return categories, medians, lower, upper


def convert_runtime(values: list[float], unit: str) -> list[float]:
    scale = {"ns": 1.0, "us": 1e-3, "ms": 1e-6}[unit]
    return [value * scale for value in values]


def configure_matplotlib() -> None:
    plt.rcParams.update({
        "font.family": "serif",
        "font.size": 8.5,
        "axes.labelsize": 8.5,
        "axes.titlesize": 8.5,
        "legend.fontsize": 7.5,
        "xtick.labelsize": 8.0,
        "ytick.labelsize": 8.0,
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "axes.spines.top": False,
        "axes.spines.right": False,
    })


def annotate_panel(axis: plt.Axes, label: str) -> None:
    axis.text(
        -0.16,
        1.04,
        label,
        transform=axis.transAxes,
        fontsize=9.5,
        fontweight="bold",
        va="bottom",
        ha="left")


def configure_feasible_cut_axis(axis: plt.Axes, x_values: list[int]) -> None:
    if not x_values:
        return
    axis.set_xscale("log", base=2)
    tick_values = x_values
    if len(x_values) > 8:
        tick_values = x_values[::2]
        if tick_values[-1] != x_values[-1]:
            tick_values.append(x_values[-1])
    axis.set_xticks(tick_values)
    axis.set_xticklabels([format_feasible_cut_tick(value) for value in tick_values])


def format_feasible_cut_tick(value: int) -> str:
    if value >= 1024 and value % 1024 == 0:
        return f"{value // 1024}K"
    return str(value)


def bucket_for_fraction(feasible_fraction_percent: float) -> str | None:
    if feasible_fraction_percent < 1.0:
        return None
    if feasible_fraction_percent < 5.0:
        return "tight"
    if feasible_fraction_percent < 20.0:
        return "medium"
    if feasible_fraction_percent <= 50.0:
        return "loose"
    return "wide"


def load_bucketed_rows(rows: list[dict[str, str]]) -> tuple[dict[str, list[dict[str, float]]], int]:
    grouped = {bucket: [] for bucket, _, _ in SCATTER_BUCKETS}
    skipped_rows = 0
    for row in rows:
        feasible_fraction_percent = float(row["feasible_fraction"]) * 100.0
        bucket = bucket_for_fraction(feasible_fraction_percent)
        if bucket is None:
            skipped_rows += 1
            continue
        selected_over_best = finite_float(row, "selected_over_best")
        solver_time_ns_mean = finite_float(row, "solver_time_ns_mean")
        if selected_over_best is None or solver_time_ns_mean is None:
            skipped_rows += 1
            continue
        grouped[bucket].append({
            "selected_over_best": selected_over_best,
            "solver_time_converted": convert_runtime([solver_time_ns_mean], "ns")[0],
        })
    return grouped, skipped_rows


def load_topology_rows(rows: list[dict[str, str]]) -> tuple[list[tuple[str, str, str]], dict[str, list[dict[str, float]]], int]:
    grouped: dict[str, list[dict[str, float]]] = defaultdict(list)
    skipped_rows = 0
    for row in rows:
        selected_over_best = finite_float(row, "selected_over_best")
        solver_time_ns_mean = finite_float(row, "solver_time_ns_mean")
        if selected_over_best is None or solver_time_ns_mean is None:
            skipped_rows += 1
            continue
        grouped[row["scenario"]].append({
            "selected_over_best": selected_over_best,
            "solver_time_converted": convert_runtime([solver_time_ns_mean], "ns")[0],
        })

    categories = sorted(grouped, key=scenario_sort_key)
    category_specs = [
        (category, scenario_label(category), CATEGORY_COLORS[index % len(CATEGORY_COLORS)])
        for index, category in enumerate(categories)
    ]
    return category_specs, grouped, skipped_rows


def parse_float(value: str) -> float:
    return float(value)


def render_heatmap(
    axis: plt.Axes,
    values: list[list[float]],
    x_labels: list[str],
    y_labels: list[str],
    *,
    colorbar_label: str,
    title: str,
    cmap: str,
    value_formatter,
    vmin: float | None = None,
    vmax: float | None = None,
) -> None:
    image = axis.imshow(values, origin="lower", aspect="auto", cmap=cmap, vmin=vmin, vmax=vmax)
    axis.set_xticks(range(len(x_labels)))
    axis.set_xticklabels(x_labels)
    axis.set_yticks(range(len(y_labels)))
    axis.set_yticklabels(y_labels)
    axis.set_xlabel("Replay initial price scale")
    axis.set_ylabel("Replay step scale")
    axis.set_title(title, pad=4.0)

    for row_index, row_values in enumerate(values):
        for column_index, value in enumerate(row_values):
            label = "--" if math.isnan(value) else value_formatter(value)
            axis.text(
                column_index,
                row_index,
                label,
                ha="center",
                va="center",
                color="#ffffff" if not math.isnan(value) and value < ((vmin or 0.0) + (vmax or value)) * 0.5 else "#1f1f1f",
                fontsize=7.0)

    colorbar = axis.figure.colorbar(image, ax=axis, fraction=0.046, pad=0.03)
    colorbar.set_label(colorbar_label)


def load_shadow_price_rows(
    rows: list[dict[str, str]],
    *,
    include_nonbinding: bool,
) -> tuple[dict[tuple[float, float], list[dict[str, str]]], list[float], list[float], int, int]:
    grouped: dict[tuple[float, float], list[dict[str, str]]] = defaultdict(list)
    replay_initial_scales: set[float] = set()
    replay_step_scales: set[float] = set()
    omitted_nonbinding = 0
    omitted_non_sweep = 0

    for row in rows:
        if row.get("benchmark_mode") != "shadow-price-sweep":
            omitted_non_sweep += 1
            continue
        if not include_nonbinding and row.get("constraints_bind") != "1":
            omitted_nonbinding += 1
            continue
        replay_initial_price_scale = parse_float(row["replay_initial_price_scale"])
        replay_step_scale = parse_float(row["replay_step_scale"])
        grouped[(replay_initial_price_scale, replay_step_scale)].append(row)
        replay_initial_scales.add(replay_initial_price_scale)
        replay_step_scales.add(replay_step_scale)

    return (
        grouped,
        sorted(replay_initial_scales),
        sorted(replay_step_scales),
        omitted_nonbinding,
        omitted_non_sweep,
    )


def summarize_series(
    rows: list[dict[str, str]],
    key: str,
    *,
    group_key: str = "iteration",
) -> tuple[list[int], list[float], list[float], list[float]]:
    grouped: dict[int, list[float]] = defaultdict(list)
    for row in rows:
        grouped[int(row[group_key])].append(float(row[key]))

    iterations = sorted(grouped)
    means = [sum(grouped[iteration]) / len(grouped[iteration]) for iteration in iterations]
    lower = [percentile(grouped[iteration], 0.25) for iteration in iterations]
    upper = [percentile(grouped[iteration], 0.75) for iteration in iterations]
    return iterations, means, lower, upper


def add_constraint_violation_columns(rows: list[dict[str, str]]) -> None:
    for row in rows:
        replay_satisfaction = float(row["replay_constraint_satisfaction_percent"])
        storage_satisfaction = float(row["storage_budget_constraint_satisfaction_percent"])
        row["replay_constraint_violation_percent"] = f"{max(0.0, 100.0 - replay_satisfaction):.12g}"
        row["storage_budget_constraint_violation_percent"] = f"{max(0.0, 100.0 - storage_satisfaction):.12g}"


def add_feasibility_columns(rows: list[dict[str, str]]) -> None:
    grouped_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped_rows[row["instance_id"]].append(row)

    for instance_rows in grouped_rows.values():
        instance_rows.sort(key=lambda row: int(row["iteration"]))
        feasible_incumbent_available = False
        for row in instance_rows:
            current_iterate_feasible = row.get("current_iterate_feasible")
            if current_iterate_feasible is None:
                current_iterate_feasible = (
                    row["replay_constraint_satisfied"] == "1"
                    and row["storage_budget_constraint_satisfied"] == "1")
            else:
                current_iterate_feasible = current_iterate_feasible == "1"

            feasible_incumbent_available = feasible_incumbent_available or current_iterate_feasible
            row["current_iterate_feasible"] = "1" if current_iterate_feasible else "0"
            row["returnable_solution_feasible"] = "1" if feasible_incumbent_available else "0"


def summarize_rate_series(
    rows: list[dict[str, str]],
    key: str,
    *,
    group_key: str = "iteration",
) -> tuple[list[int], list[float]]:
    iterations, means, _, _ = summarize_series(rows, key, group_key=group_key)
    return iterations, [value * 100.0 for value in means]


def summarize_convergence(rows: list[dict[str, str]]) -> dict[str, list[float] | list[int]]:
    iterations, replay_mean, replay_p25, replay_p75 = summarize_series(rows, "replay_constraint_violation_percent")
    budget_iterations, budget_mean, budget_p25, budget_p75 = summarize_series(
        rows,
        "storage_budget_constraint_violation_percent")
    if iterations != budget_iterations:
        raise ValueError("shadow-price convergence CSV has mismatched replay and storage iteration coverage")

    returnable_iterations, returnable_feasible_rate = summarize_rate_series(rows, "returnable_solution_feasible")
    if iterations != returnable_iterations:
        raise ValueError("shadow-price convergence CSV has mismatched iteration coverage in feasibility series")

    return {
        "iterations": iterations,
        "replay_mean": replay_mean,
        "replay_p25": replay_p25,
        "replay_p75": replay_p75,
        "budget_mean": budget_mean,
        "budget_p25": budget_p25,
        "budget_p75": budget_p75,
        "returnable_feasible_rate": returnable_feasible_rate,
    }


def render_convergence_axes(
    violation_axis: plt.Axes,
    feasibility_axis: plt.Axes,
    summary: dict[str, list[float] | list[int]],
    *,
    max_violation: float,
    title: str | None = None,
) -> None:
    iterations = summary["iterations"]
    replay_mean = summary["replay_mean"]
    replay_p25 = summary["replay_p25"]
    replay_p75 = summary["replay_p75"]
    budget_mean = summary["budget_mean"]
    budget_p25 = summary["budget_p25"]
    budget_p75 = summary["budget_p75"]
    returnable_feasible_rate = summary["returnable_feasible_rate"]

    violation_axis.axhline(0.0, color=COLOR_BASELINE, linestyle=(0, (4, 2)), linewidth=1.0, label="fully satisfied")
    violation_axis.plot(iterations, replay_mean, marker="o", markersize=3.8, linewidth=1.8, color="#1b9e77", label="replay")
    violation_axis.fill_between(iterations, replay_p25, replay_p75, color="#1b9e77", alpha=0.16)
    violation_axis.plot(
        iterations,
        budget_mean,
        marker="s",
        markersize=3.6,
        linewidth=1.7,
        color="#d95f02",
        label="storage budget")
    violation_axis.fill_between(iterations, budget_p25, budget_p75, color="#d95f02", alpha=0.14)
    violation_axis.set_xlim(min(iterations), max(iterations))
    violation_axis.set_ylim(0.0, max(1.0, max_violation * 1.06))
    violation_axis.grid(axis="y", alpha=0.22, linewidth=0.7)
    if title is not None:
        violation_axis.set_title(title, pad=4.0)

    feasibility_axis.plot(
        iterations,
        returnable_feasible_rate,
        marker="D",
        markersize=3.4,
        linewidth=1.8,
        color="#1f78b4",
        label="feasible solution available")
    feasibility_axis.set_xticks(iterations)
    feasibility_axis.set_xlim(min(iterations), max(iterations))
    feasibility_axis.set_ylim(0.0, 100.0)
    feasibility_axis.grid(axis="y", alpha=0.22, linewidth=0.7)


def plot_category_scatter(
    axis: plt.Axes,
    categories: list[tuple[str, str, str]],
    grouped_rows: dict[str, list[dict[str, float]]],
    key: str,
    ylabel: str,
    *,
    baseline: float | None = None,
) -> None:
    rng = random.Random(RNG_SEED)
    all_values: list[float] = []
    xtick_labels: list[str] = []

    for bucket_index, (bucket_name, bucket_label, color) in enumerate(categories):
        rows = grouped_rows[bucket_name]
        values = [row[key] for row in rows]
        all_values.extend(values)
        xtick_labels.append(bucket_label)
        if not values:
            continue

        x_values = [bucket_index + rng.uniform(-JITTER_WIDTH, JITTER_WIDTH) for _ in values]
        axis.scatter(
            x_values,
            values,
            s=POINT_SIZE,
            color=color,
            alpha=POINT_ALPHA,
            edgecolors="none")

        median_value = statistics.median(values)
        axis.hlines(median_value, bucket_index - 0.22, bucket_index + 0.22, color="#2f2f2f", linewidth=1.35, zorder=3)
        axis.plot(bucket_index, median_value, marker="o", markersize=3.2, color="#2f2f2f", zorder=4)

    axis.set_xticks(range(len(categories)))
    axis.set_xticklabels(xtick_labels)
    axis.set_ylabel(ylabel)
    axis.grid(axis="y", alpha=0.22, linewidth=0.7)
    axis.set_xlim(-0.5, len(categories) - 0.5)

    if baseline is not None:
        axis.axhline(baseline, color=COLOR_BASELINE, linestyle=(0, (4, 2)), linewidth=1.0)

    if not all_values:
        return

    min_value = min(all_values)
    max_value = max(all_values)
    if baseline is not None:
        min_value = min(min_value, baseline)
        max_value = max(max_value, baseline)
    span = max_value - min_value
    padding = 0.06 * span if span > 0.0 else max(0.02 * max_value, 0.05)
    lower = min_value - padding
    upper = max_value + padding
    if baseline is not None:
        lower = min(lower, baseline - padding)
        upper = max(upper, baseline + padding)
    axis.set_ylim(lower, upper)


def render_paper_plot(args: argparse.Namespace, rows: list[dict[str, str]]) -> int:
    filtered_rows = select_rows_for_plot(
        rows,
        plot_kind=args.plot_kind,
        requested_mode=args.baseline_mode,
        requires_exact_baseline=True)
    if not filtered_rows:
        print("paper plot requires exact-baseline rows; none matched the requested filters", file=sys.stderr)
        return 1

    if args.group_by == "scenario":
        selected_x, selected_median, selected_p25, selected_p75 = summarize_categorical(filtered_rows, "selected_over_best")
        avg_x, avg_median, avg_p25, avg_p75 = summarize_categorical(filtered_rows, "avg_over_best")
        worst_x, worst_median, worst_p25, worst_p75 = summarize_categorical(filtered_rows, "worst_over_best")
        runtime_x, runtime_median, runtime_p25, runtime_p75 = summarize_categorical(filtered_rows, "solver_time_ns_mean")
    else:
        selected_x, selected_median, selected_p25, selected_p75 = summarize(filtered_rows, "selected_over_best")
        avg_x, avg_median, avg_p25, avg_p75 = summarize(filtered_rows, "avg_over_best")
        worst_x, worst_median, worst_p25, worst_p75 = summarize(filtered_rows, "worst_over_best")
        runtime_x, runtime_median, runtime_p25, runtime_p75 = summarize(filtered_rows, "solver_time_ns_mean")

    if not selected_x or not runtime_x:
        print("paper plot does not have enough finite exact-baseline data to render", file=sys.stderr)
        return 1

    runtime_median = convert_runtime(runtime_median, args.runtime_unit)
    runtime_p25 = convert_runtime(runtime_p25, args.runtime_unit)
    runtime_p75 = convert_runtime(runtime_p75, args.runtime_unit)

    figure, (main_axis, runtime_axis) = plt.subplots(1, 2, figsize=(args.width, args.height), constrained_layout=True)

    if args.group_by == "scenario":
        main_positions = list(range(len(selected_x)))
        main_axis.axhline(1.0, color=COLOR_BASELINE, linestyle=(0, (4, 2)), linewidth=1.1, label="optimal feasible cut")
        main_axis.plot(main_positions, selected_median, marker="o", markersize=4.0, linewidth=1.8, color=COLOR_SELECTED, label="selected")
        main_axis.fill_between(main_positions, selected_p25, selected_p75, color=COLOR_SELECTED, alpha=0.16)
        main_axis.plot(main_positions, avg_median, marker="s", markersize=3.7, linewidth=1.5, color=COLOR_AVERAGE, label="average feasible")
        main_axis.fill_between(main_positions, avg_p25, avg_p75, color=COLOR_AVERAGE, alpha=0.14)
        main_axis.plot(main_positions, worst_median, marker="^", markersize=3.8, linewidth=1.5, color=COLOR_WORST, label="worst feasible")
        main_axis.fill_between(main_positions, worst_p25, worst_p75, color=COLOR_WORST, alpha=0.14)
        main_axis.set_xticks(main_positions)
        main_axis.set_xticklabels([scenario_label(category) for category in selected_x])
        main_axis.set_xlabel("Topology family")
    else:
        main_axis.axhline(1.0, color=COLOR_BASELINE, linestyle=(0, (4, 2)), linewidth=1.1, label="optimal feasible cut")
        main_axis.plot(selected_x, selected_median, marker="o", markersize=4.0, linewidth=1.8, color=COLOR_SELECTED, label="selected")
        main_axis.fill_between(selected_x, selected_p25, selected_p75, color=COLOR_SELECTED, alpha=0.16)
        main_axis.plot(avg_x, avg_median, marker="s", markersize=3.7, linewidth=1.5, color=COLOR_AVERAGE, label="average feasible")
        main_axis.fill_between(avg_x, avg_p25, avg_p75, color=COLOR_AVERAGE, alpha=0.14)
        main_axis.plot(worst_x, worst_median, marker="^", markersize=3.8, linewidth=1.5, color=COLOR_WORST, label="worst feasible")
        main_axis.fill_between(worst_x, worst_p25, worst_p75, color=COLOR_WORST, alpha=0.14)
        configure_feasible_cut_axis(main_axis, selected_x)
        main_axis.set_xlabel("Feasible cuts")
    main_axis.set_ylabel("Normalized maintenance cost")
    main_axis.grid(axis="y", alpha=0.22, linewidth=0.7)
    main_axis.set_ylim(0.98, max(worst_p75) * 1.06)
    main_axis.legend(loc="upper left", ncols=2, frameon=False, handlelength=2.2, columnspacing=1.2)
    annotate_panel(main_axis, "(a)")

    if args.group_by == "scenario":
        runtime_positions = list(range(len(runtime_x)))
        runtime_axis.plot(runtime_positions, runtime_median, marker="o", markersize=4.0, linewidth=1.8, color="#1f78b4")
        runtime_axis.fill_between(runtime_positions, runtime_p25, runtime_p75, color="#1f78b4", alpha=0.16)
        runtime_axis.set_xticks(runtime_positions)
        runtime_axis.set_xticklabels([scenario_label(category) for category in runtime_x])
        runtime_axis.set_xlabel("Topology family")
    else:
        runtime_axis.plot(runtime_x, runtime_median, marker="o", markersize=4.0, linewidth=1.8, color="#1f78b4")
        runtime_axis.fill_between(runtime_x, runtime_p25, runtime_p75, color="#1f78b4", alpha=0.16)
        configure_feasible_cut_axis(runtime_axis, runtime_x)
        runtime_axis.set_xlabel("Feasible cuts")
    runtime_axis.set_ylabel(f"Mean solver time [{args.runtime_unit}]")
    runtime_axis.grid(axis="y", alpha=0.22, linewidth=0.7)
    runtime_axis.set_ylim(0.0, max(runtime_p75) * 1.10 if max(runtime_p75) > 0.0 else 1.0)
    annotate_panel(runtime_axis, "(b)")

    figure.savefig(args.output_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    return 0


def render_scatter_plot(args: argparse.Namespace, rows: list[dict[str, str]]) -> int:
    filtered_rows = select_rows_for_plot(
        rows,
        plot_kind=args.plot_kind,
        requested_mode=args.baseline_mode,
        requires_exact_baseline=True)
    if not filtered_rows:
        print("scatter plot requires exact-baseline rows; none matched the requested filters", file=sys.stderr)
        return 1

    if args.group_by == "scenario":
        categories, grouped_rows, skipped_rows = load_topology_rows(filtered_rows)
    else:
        categories = SCATTER_BUCKETS
        grouped_rows, skipped_rows = load_bucketed_rows(filtered_rows)
    for bucket_rows in grouped_rows.values():
        converted = convert_runtime([row["solver_time_converted"] for row in bucket_rows], args.runtime_unit)
        for row, converted_value in zip(bucket_rows, converted, strict=True):
            row["solver_time_converted"] = converted_value

    figure, (cost_axis, runtime_axis) = plt.subplots(1, 2, figsize=(args.width, max(args.height, 2.75)), constrained_layout=True)

    plot_category_scatter(cost_axis, categories, grouped_rows, "selected_over_best", "Normalized maintenance cost", baseline=1.0)
    cost_axis.set_xlabel("Topology family" if args.group_by == "scenario" else "Feasible-cut bucket")
    annotate_panel(cost_axis, "(a)")

    plot_category_scatter(runtime_axis, categories, grouped_rows, "solver_time_converted", f"Mean solver time [{args.runtime_unit}]")
    runtime_axis.set_xlabel("Topology family" if args.group_by == "scenario" else "Feasible-cut bucket")
    annotate_panel(runtime_axis, "(b)")

    if skipped_rows > 0:
        figure.text(
            0.5,
            0.005,
            f"Omitted {skipped_rows} rows without finite exact-baseline metrics.",
            ha="center",
            va="bottom",
            fontsize=7.0)

    figure.savefig(args.output_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    return 0


def render_shadow_price_heatmap(args: argparse.Namespace, rows: list[dict[str, str]]) -> int:
    filtered_rows = select_rows_for_plot(
        rows,
        plot_kind=args.plot_kind,
        requested_mode=args.baseline_mode,
        requires_exact_baseline=True)
    if not filtered_rows:
        print("shadow-price heatmap requires exact-baseline sweep rows; none matched the requested filters", file=sys.stderr)
        return 1

    grouped_rows, replay_initial_scales, replay_step_scales, omitted_nonbinding, omitted_non_sweep = load_shadow_price_rows(
        filtered_rows,
        include_nonbinding=args.include_nonbinding)
    if not grouped_rows:
        print("input CSV does not contain any matching shadow-price sweep rows", file=sys.stderr)
        return 1

    hit_rate_values: list[list[float]] = []
    mean_cost_values: list[list[float]] = []
    successful_rows = 0
    total_rows = 0

    for replay_step_scale in replay_step_scales:
        hit_rate_row: list[float] = []
        mean_cost_row: list[float] = []
        for replay_initial_price_scale in replay_initial_scales:
            cell_rows = grouped_rows[(replay_initial_price_scale, replay_step_scale)]
            total_rows += len(cell_rows)
            successful_cell_rows = [row for row in cell_rows if row["solver_succeeded"] == "1"]
            successful_rows += len(successful_cell_rows)

            if not cell_rows:
                hit_rate_row.append(float("nan"))
            else:
                hit_rate_row.append(
                    100.0 * sum(1 for row in cell_rows if row["optimal_hit"] == "1") / float(len(cell_rows)))

            successful_costs = [parse_float(row["selected_over_best"]) for row in successful_cell_rows if not math.isnan(parse_float(row["selected_over_best"]))]
            if successful_costs:
                mean_cost_row.append(sum(successful_costs) / len(successful_costs))
            else:
                mean_cost_row.append(float("nan"))
        hit_rate_values.append(hit_rate_row)
        mean_cost_values.append(mean_cost_row)

    finite_cost_values = [value for row_values in mean_cost_values for value in row_values if not math.isnan(value)]
    if not finite_cost_values:
        print("shadow-price sweep CSV does not contain any successful solver rows", file=sys.stderr)
        return 1

    x_labels = [f"{scale:g}x" for scale in replay_initial_scales]
    y_labels = [f"{scale:g}x" for scale in replay_step_scales]
    max_cost_value = max(finite_cost_values)

    figure, (hit_rate_axis, cost_axis) = plt.subplots(
        1,
        2,
        figsize=(args.width, max(args.height, 2.9)),
        constrained_layout=True)

    render_heatmap(
        hit_rate_axis,
        hit_rate_values,
        x_labels,
        y_labels,
        colorbar_label="Optimal-hit rate [%]",
        title="Optimal-hit rate",
        cmap="viridis",
        value_formatter=lambda value: f"{value:.0f}",
        vmin=0.0,
        vmax=100.0)
    annotate_panel(hit_rate_axis, "(a)")

    render_heatmap(
        cost_axis,
        mean_cost_values,
        x_labels,
        y_labels,
        colorbar_label="Mean selected / best",
        title="Mean normalized maintenance cost",
        cmap="magma",
        value_formatter=lambda value: f"{value:.3f}",
        vmin=1.0,
        vmax=max(1.0, max_cost_value))
    annotate_panel(cost_axis, "(b)")

    binding_label = "all frozen scenarios" if args.include_nonbinding else "constraint-binding frozen scenarios"
    topology_count = len({row["scenario"] for row in filtered_rows if row.get("benchmark_mode") == "shadow-price-sweep"})
    footer = [
        f"Shadow-price sweep over {total_rows} {binding_label} across {topology_count} topology families; {successful_rows} solver runs succeeded."
    ]
    if omitted_nonbinding > 0 and not args.include_nonbinding:
        footer.append(f"Omitted {omitted_nonbinding} non-binding sweep rows.")
    if omitted_non_sweep > 0:
        footer.append(f"Ignored {omitted_non_sweep} non-sweep rows.")
    figure.text(0.5, 0.005, " ".join(footer), ha="center", va="bottom", fontsize=7.0)

    figure.savefig(args.output_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    return 0


def render_shadow_price_convergence(args: argparse.Namespace, rows: list[dict[str, str]]) -> int:
    convergence_rows = [
        row
        for row in select_rows_for_plot(
            rows,
            plot_kind=args.plot_kind,
            requested_mode=args.baseline_mode,
            requires_exact_baseline=False)
        if "replay_constraint_satisfaction_percent" in row
    ]
    if not convergence_rows:
        print("input CSV does not contain any shadow-price convergence rows", file=sys.stderr)
        return 1
    add_constraint_violation_columns(convergence_rows)
    add_feasibility_columns(convergence_rows)

    replay_initial_scales = {row["replay_initial_price_scale"] for row in convergence_rows}
    replay_step_scales = {row["replay_step_scale"] for row in convergence_rows}
    if len(replay_initial_scales) != 1 or len(replay_step_scales) != 1:
        print("shadow-price convergence plot expects exactly one replay initial scale and one replay step scale", file=sys.stderr)
        return 1

    try:
        summary = summarize_convergence(convergence_rows)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1

    max_violation = max(max(summary["replay_p75"]), max(summary["budget_p75"]))

    figure, (violation_axis, feasibility_axis) = plt.subplots(
        2,
        1,
        figsize=(args.width, max(args.height, 4.35)),
        constrained_layout=True,
        sharex=True)

    annotate_panel(violation_axis, "A")
    annotate_panel(feasibility_axis, "B")

    render_convergence_axes(
        violation_axis,
        feasibility_axis,
        summary,
        max_violation=max_violation)
    violation_axis.set_ylabel("Violation [%]")
    feasibility_axis.set_xlabel("Shadow-price iteration")
    feasibility_axis.set_ylabel("Feasible [%]")
    violation_axis.legend(loc="upper right", frameon=False)
    feasibility_axis.legend(loc="lower right", frameon=False)

    unique_instances = {row["instance_id"] for row in convergence_rows}
    tuning_label = (
        f"replay initial {next(iter(replay_initial_scales))}x, replay step {next(iter(replay_step_scales))}x, "
        f"storage step {convergence_rows[0]['storage_step_scale']}x"
    )
    figure.text(
        0.5,
        0.005,
        "Top: current-iterate replay and storage-budget violation. "
        "Bottom: fraction of scenarios where the solver already has a feasible solution to return. "
        f"Aggregated over {len(unique_instances)} frozen scenarios and {len({row['scenario'] for row in convergence_rows})} topology families; {tuning_label}.",
        ha="center",
        va="bottom",
        fontsize=7.0)

    figure.savefig(args.output_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    return 0


def main() -> int:
    args = parse_args()
    configure_matplotlib()

    with args.input_csv.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        print("input CSV is empty", file=sys.stderr)
        return 1

    if args.plot_kind == "shadow-price-convergence":
        return render_shadow_price_convergence(args, rows)
    if args.plot_kind == "shadow-price-heatmap":
        return render_shadow_price_heatmap(args, rows)
    if args.plot_kind == "scatter":
        return render_scatter_plot(args, rows)
    return render_paper_plot(args, rows)


if __name__ == "__main__":
    raise SystemExit(main())
