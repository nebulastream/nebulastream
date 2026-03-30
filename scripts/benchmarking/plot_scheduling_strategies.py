#!/usr/bin/env python3
"""Visualize all scheduling strategies and how tasks flow through queues."""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch
import numpy as np

def draw_queue(ax, x, y, w, h, label, color="#E8E8E8", border="#333333"):
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02",
                         facecolor=color, edgecolor=border, linewidth=1.5)
    ax.add_patch(box)
    ax.text(x + w/2, y + h/2, label, ha="center", va="center", fontsize=7, fontweight="bold")

def draw_thread(ax, x, y, w, h, label, color="#D4E8FF"):
    box = FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02",
                         facecolor=color, edgecolor="#2266AA", linewidth=1.5)
    ax.add_patch(box)
    ax.text(x + w/2, y + h/2, label, ha="center", va="center", fontsize=7, fontweight="bold")

def draw_arrow(ax, x1, y1, x2, y2, color="#333333", style="-|>", lw=1.5, ls="-"):
    arrow = FancyArrowPatch((x1, y1), (x2, y2),
                            arrowstyle=style, color=color,
                            linewidth=lw, linestyle=ls,
                            mutation_scale=12,
                            connectionstyle="arc3,rad=0.0")
    ax.add_patch(arrow)

def draw_curved_arrow(ax, x1, y1, x2, y2, color="#333333", rad=0.2, lw=1.5, ls="-"):
    arrow = FancyArrowPatch((x1, y1), (x2, y2),
                            arrowstyle="-|>", color=color,
                            linewidth=lw, linestyle=ls,
                            mutation_scale=12,
                            connectionstyle=f"arc3,rad={rad}")
    ax.add_patch(arrow)

def draw_source(ax, x, y, label="Source"):
    box = FancyBboxPatch((x, y), 0.12, 0.06, boxstyle="round,pad=0.01",
                         facecolor="#FFE0B2", edgecolor="#E65100", linewidth=1.5)
    ax.add_patch(box)
    ax.text(x + 0.06, y + 0.03, label, ha="center", va="center", fontsize=6, fontweight="bold")

def draw_panel(ax, title, strategy_type):
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.set_aspect("equal")
    ax.axis("off")
    ax.set_title(title, fontsize=10, fontweight="bold", pad=8)

    n_threads = 4
    qw, qh = 0.08, 0.045  # queue box size
    tw, th = 0.08, 0.045  # thread box size
    thread_spacing = 0.18
    start_x = 0.15

    # Source
    draw_source(ax, 0.42, 0.92)

    if strategy_type == "global":
        # Single shared queue
        draw_queue(ax, 0.38, 0.72, 0.24, 0.06, "Shared Queue", color="#C8E6C9", border="#2E7D32")
        # Arrow from source to queue
        draw_arrow(ax, 0.48, 0.92, 0.50, 0.78, color="#E65100")

        # Threads pulling from shared queue
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_thread(ax, tx, 0.50, tw, th, f"T{i}")
            draw_arrow(ax, 0.50, 0.72, tx + tw/2, 0.545, color="#2266AA")

        # Successor goes back to shared queue
        draw_curved_arrow(ax, start_x + tw/2, 0.55, 0.38, 0.74, color="#888888", rad=-0.3, ls="--")
        ax.text(0.15, 0.64, "successor", fontsize=5.5, color="#888888", style="italic")

    elif strategy_type == "round_robin":
        # Per-thread queues
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_queue(ax, tx, 0.72, qw, qh, f"Q{i}", color="#C8E6C9", border="#2E7D32")
            draw_thread(ax, tx, 0.50, tw, th, f"T{i}")
            draw_arrow(ax, tx + qw/2, 0.72, tx + tw/2, 0.545, color="#2266AA")

        # Source distributes round-robin
        draw_arrow(ax, 0.48, 0.92, 0.48, 0.82, color="#E65100")
        ax.text(0.50, 0.84, "round\nrobin", fontsize=5.5, ha="center", color="#E65100")
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_curved_arrow(ax, 0.48, 0.82, tx + qw/2, 0.765, color="#E65100", rad=0.1, lw=1)

        # Successor: round-robin to any queue
        draw_curved_arrow(ax, start_x + tw/2, 0.55, start_x + thread_spacing + qw/2, 0.72,
                         color="#888888", rad=-0.2, ls="--")
        ax.text(0.12, 0.62, "successor\n(round-robin)", fontsize=5, color="#888888", style="italic")

    elif strategy_type == "smallest":
        # Per-thread queues with sizes
        sizes = [2, 5, 1, 3]
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_queue(ax, tx, 0.72, qw, qh, f"Q{i} [{sizes[i]}]", color="#C8E6C9", border="#2E7D32")
            draw_thread(ax, tx, 0.50, tw, th, f"T{i}")
            draw_arrow(ax, tx + qw/2, 0.72, tx + tw/2, 0.545, color="#2266AA")

        # Source distributes round-robin for admission
        draw_arrow(ax, 0.48, 0.92, 0.48, 0.82, color="#E65100")
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_curved_arrow(ax, 0.48, 0.82, tx + qw/2, 0.765, color="#E65100", rad=0.1, lw=1)

        # Highlight smallest queue (Q2)
        highlight = FancyBboxPatch((start_x + 2*thread_spacing - 0.01, 0.715),
                                   qw + 0.02, qh + 0.01,
                                   boxstyle="round,pad=0.01",
                                   facecolor="none", edgecolor="#FF0000", linewidth=2, linestyle="--")
        ax.add_patch(highlight)

        # Successor goes to smallest
        draw_curved_arrow(ax, start_x + tw/2, 0.55, start_x + 2*thread_spacing + qw/2, 0.72,
                         color="#FF0000", rad=-0.3, ls="--")
        ax.text(0.12, 0.62, "successor\n(to smallest)", fontsize=5, color="#FF0000", style="italic")

    elif strategy_type == "choose_two":
        # Per-thread queues with sizes
        sizes = [2, 5, 1, 3]
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_queue(ax, tx, 0.72, qw, qh, f"Q{i} [{sizes[i]}]", color="#C8E6C9", border="#2E7D32")
            draw_thread(ax, tx, 0.50, tw, th, f"T{i}")
            draw_arrow(ax, tx + qw/2, 0.72, tx + tw/2, 0.545, color="#2266AA")

        # Source distributes round-robin for admission
        draw_arrow(ax, 0.48, 0.92, 0.48, 0.82, color="#E65100")
        for i in range(n_threads):
            tx = start_x + i * thread_spacing
            draw_curved_arrow(ax, 0.48, 0.82, tx + qw/2, 0.765, color="#E65100", rad=0.1, lw=1)

        # Highlight two random candidates (Q1 and Q2), pick Q2 (smaller)
        for idx in [1, 2]:
            highlight = FancyBboxPatch((start_x + idx*thread_spacing - 0.01, 0.715),
                                       qw + 0.02, qh + 0.01,
                                       boxstyle="round,pad=0.01",
                                       facecolor="none",
                                       edgecolor="#9C27B0" if idx == 1 else "#FF0000",
                                       linewidth=2, linestyle="--")
            ax.add_patch(highlight)

        draw_curved_arrow(ax, start_x + tw/2, 0.55, start_x + 2*thread_spacing + qw/2, 0.72,
                         color="#FF0000", rad=-0.3, ls="--")
        ax.text(0.12, 0.62, "successor\n(shorter of 2)", fontsize=5, color="#FF0000", style="italic")
        ax.text(0.50, 0.67, "random pick", fontsize=5, color="#9C27B0", ha="center")

    # Flags section
    flag_y = 0.38
    if strategy_type != "global":
        # Producer-local
        ax.plot([0.05, 0.95], [flag_y + 0.08, flag_y + 0.08], color="#CCCCCC", lw=0.5)
        ax.text(0.50, flag_y + 0.10, "Optional flags:", fontsize=6, ha="center", color="#666666")

        # Producer local illustration
        ax.text(0.08, flag_y, "+PL (Producer Local):", fontsize=6, fontweight="bold", color="#00695C")
        ax.text(0.08, flag_y - 0.05, "T0 produces successor", fontsize=5.5, color="#555555")
        draw_curved_arrow(ax, 0.55, flag_y + 0.01, 0.55, flag_y - 0.04, color="#00695C", rad=0.4, lw=1.5)
        ax.text(0.58, flag_y - 0.02, "stays on Q0", fontsize=5.5, color="#00695C", fontweight="bold")

        # Work stealing illustration
        ax.text(0.08, flag_y - 0.12, "+WS (Work Stealing):", fontsize=6, fontweight="bold", color="#BF360C")
        ax.text(0.08, flag_y - 0.17, "T3 idle, steals from T0", fontsize=5.5, color="#555555")
        draw_curved_arrow(ax, 0.65, flag_y - 0.11, 0.55, flag_y - 0.16, color="#BF360C", rad=-0.2, lw=1.5, ls="--")
        ax.text(0.58, flag_y - 0.19, "steal task", fontsize=5.5, color="#BF360C", fontweight="bold")


fig, axes = plt.subplots(2, 2, figsize=(16, 14))

draw_panel(axes[0, 0], "GLOBAL_QUEUE\n(single shared queue, all threads contend)", "global")
draw_panel(axes[0, 1], "PER_THREAD_ROUND_ROBIN\n(tasks distributed in rotating order)", "round_robin")
draw_panel(axes[1, 0], "PER_THREAD_SMALLEST_QUEUE\n(tasks go to queue with fewest items)", "smallest")
draw_panel(axes[1, 1], "PER_THREAD_CHOOSE_TWO\n(pick shorter of 2 random queues)", "choose_two")

fig.suptitle("Scheduling Strategies Overview", fontsize=16, fontweight="bold", y=0.98)

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig("scheduling_strategies.png", dpi=150, bbox_inches="tight")
print("Chart saved to: scheduling_strategies.png")
