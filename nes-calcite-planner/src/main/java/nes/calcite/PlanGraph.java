package nes.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

/**
 * A plan as a Graphviz graph (DOT), for inspection: one box per operator, arrows in the direction the rows flow. The parts
 * of a placed plan are boxes per worker, joined by their network channels; with pipelines, each pipeline is a box too.
 */
final class PlanGraph {
    private PlanGraph() {}

    /**
     * A plan, or one part of a placed plan: the worker that runs it (null: not placed), and where its rows go if the plan
     * does not end in a sink operator itself (a logical part: null for physical parts).
     */
    record Part(String worker, RelNode plan, Output output) {}

    /** The rows of a logical part go into the query's sink (channel null), or over the channel to another worker. */
    record Output(String channel, String downstreamHost) {}

    static String of(RelNode plan, Predicate<RelNode> highlighted) {
        return of(List.of(new Part(null, plan, null)), highlighted, false);
    }

    static String of(List<Part> parts, Predicate<RelNode> highlighted, boolean pipelines) {
        StringBuilder dot = new StringBuilder("digraph plan {\n  rankdir=BT;\n  newrank=true;\n"
            + "  node [shape=box, style=\"rounded,filled\", fontname=\"Helvetica\", fontsize=10];\n"
            + "  edge [color=\"#555555\"];\n");
        Map<RelNode, String> ids = new IdentityHashMap<>();
        List<String> edges = new ArrayList<>();
        Map<String, String> senders = new HashMap<>();
        Map<String, String> receivers = new HashMap<>();
        /// One box per worker, holding all its parts, connected or not.
        Map<String, List<Integer>> byWorker = new java.util.LinkedHashMap<>();
        for (int p = 0; p < parts.size(); p++) {
            byWorker.computeIfAbsent(parts.get(p).worker(), w -> new ArrayList<>()).add(p);
        }
        int boxes = 0;
        for (Map.Entry<String, List<Integer>> worker : byWorker.entrySet()) {
            if (worker.getKey() != null) {
                dot.append("  subgraph cluster_worker").append(boxes++).append(" {\n    label=\"worker ").append(escape(worker.getKey()))
                    .append("\"; style=\"rounded,dashed\"; color=\"#888888\"; fontname=\"Helvetica\"; fontsize=12;\n");
            }
            for (int p : worker.getValue()) {
                Part part = parts.get(p);
                Map<Integer, List<String>> byPipeline = new java.util.TreeMap<>();
                Map<RelNode, Integer> pipelineOf = new IdentityHashMap<>();
                int[] nextPipeline = {0};
                String top = write(part.plan(), "p" + p + "_", ids, dot, edges, highlighted, senders, receivers,
                    pipelines ? byPipeline : null, pipelineOf, nextPipeline);
                if (part.output() != null) {
                    /// A logical part has no sink operator: its end is drawn as one.
                    String end = "p" + p + "_end";
                    boolean network = part.output().channel() != null;
                    dot.append("    ").append(end).append(" [label=\"")
                        .append(network ? "NetworkSink\\nto=" + escape(part.output().downstreamHost()) : "Sink")
                        .append("\", fillcolor=\"").append(network ? "#fed7aa" : "#e5e7eb").append("\", color=\"#9ca3af\"];\n");
                    edges.add(top + " -> " + end);
                    if (network) {
                        senders.put(part.output().channel(), end);
                    }
                }
                int partIndex = p;
                byPipeline.forEach((pipeline, nodes) -> dot.append("    subgraph cluster_p").append(partIndex).append("_pipeline")
                    .append(pipeline).append(" { label=\"pipeline\"; style=\"rounded\"; color=\"#2563eb\"; fontcolor=\"#2563eb\"; ")
                    .append(String.join("; ", nodes)).append(" }\n"));
            }
            if (worker.getKey() != null) {
                dot.append("  }\n");
            }
        }
        edges.forEach(edge -> dot.append("  ").append(edge).append(";\n"));
        senders.forEach((channel, sender) -> {
            if (receivers.containsKey(channel)) {
                dot.append("  ").append(sender).append(" -> ").append(receivers.get(channel))
                    .append(" [style=dashed, color=\"#dc2626\", label=\"network\", fontcolor=\"#dc2626\", fontsize=9];\n");
            }
        });
        return dot.append("}\n").toString();
    }

    private static String write(RelNode original, String prefix, Map<RelNode, String> ids, StringBuilder dot, List<String> edges,
                                Predicate<RelNode> highlighted, Map<String, String> senders, Map<String, String> receivers,
                                Map<Integer, List<String>> byPipeline, Map<RelNode, Integer> pipelineOf, int[] nextPipeline) {
        RelNode node = unwrap(original);
        String existing = ids.get(node);
        if (existing != null) {
            return existing;
        }
        String id = prefix + ids.size();
        ids.put(node, id);
        List<String> inputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            inputs.add(write(input, prefix, ids, dot, edges, highlighted, senders, receivers, byPipeline, pipelineOf, nextPipeline));
        }
        /// Subqueries in the operator's expressions (before decorrelation): each plan in a box of its own, into the operator.
        List<RexSubQuery> subqueries = new ArrayList<>();
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                subqueries.add(subQuery);
                return subQuery;
            }
        });
        for (int k = 0; k < subqueries.size(); k++) {
            RexSubQuery subquery = subqueries.get(k);
            dot.append("    subgraph cluster_").append(id).append("_subquery").append(k).append(" {\n    label=\"subquery ").append(k + 1)
                .append(" (").append(escape(subquery.getOperator().getName())).append(")\"; style=\"rounded,dotted\"; color=\"#7c3aed\"; fontcolor=\"#7c3aed\";\n");
            String top = write(subquery.rel, id + "_s" + k + "_", ids, dot, edges, highlighted, senders, receivers, null, pipelineOf, nextPipeline);
            dot.append("    }\n");
            edges.add(top + " -> " + id + " [style=dotted, color=\"#7c3aed\", label=\"subquery " + (k + 1) + "\", fontcolor=\"#7c3aed\", fontsize=9]");
        }
        boolean emphasized = highlighted.test(node);
        dot.append("    ").append(id).append(" [label=\"").append(escape(label(node, subqueries))).append("\", fillcolor=\"").append(color(node))
            .append('"').append(emphasized ? ", color=\"#dc2626\", penwidth=3" : ", color=\"#9ca3af\"").append("];\n");
        inputs.forEach(input -> edges.add(input + " -> " + id));
        if (node instanceof NesPhysical.NetworkSink sink) {
            senders.put(sink.channel, id);
        } else if (node instanceof NesPhysical.NetworkSource source) {
            receivers.put(source.channel, id);
        } else if (node instanceof NesLogical.NetworkSource source) {
            receivers.put(source.channel, id);
        }
        if (byPipeline != null && node instanceof NesPhysical.NesRel) {
            boolean starts = node.getInputs().isEmpty() || node instanceof NesPhysical.Scan || node instanceof NesPhysical.Sink
                || node instanceof NesPhysical.NetworkSink || node instanceof NesPhysical.WindowProbe;
            int pipeline = starts ? nextPipeline[0]++ : pipelineOf.get(unwrap(node.getInput(0)));
            pipelineOf.put(node, pipeline);
            byPipeline.computeIfAbsent(pipeline, k -> new ArrayList<>()).add(id);
        }
        return id;
    }

    /** The operator behind a planner's placeholder (HepPlanner's vertices, VolcanoPlanner's subsets). */
    static RelNode unwrap(RelNode node) {
        if (node instanceof HepRelVertex vertex) {
            return vertex.getCurrentRel();
        }
        return node instanceof RelSubset subset ? subset.getBest() : node;
    }

    /**
     * E.g. "LogicalFilter\ncondition=[>($1, 20)]": the operator and its attributes (as in its explain output), without its
     * inputs; a subquery in an attribute is named after its box, e.g. "EXISTS(subquery 1)".
     */
    private static String label(RelNode node, List<RexSubQuery> subqueries) {
        List<org.apache.calcite.util.Pair<String, Object>> terms = new ArrayList<>();
        node.explain(new org.apache.calcite.rel.externalize.RelWriterImpl(new java.io.PrintWriter(java.io.Writer.nullWriter())) {
            @Override
            protected void explain_(RelNode rel, List<org.apache.calcite.util.Pair<String, Object>> values) {
                terms.addAll(values);
            }
        });
        StringBuilder label = new StringBuilder(node.getRelTypeName());
        for (org.apache.calcite.util.Pair<String, Object> term : terms) {
            if (term.right instanceof RelNode) {
                continue;
            }
            String value = String.valueOf(term.right);
            for (int k = 0; k < subqueries.size(); k++) {
                value = value.replace(subqueries.get(k).toString(), subqueries.get(k).getOperator().getName() + "(subquery " + (k + 1) + ")");
            }
            label.append('\n').append(wrap(term.left + "=[" + value.replace('\n', ' ') + "]"));
        }
        return label.toString();
    }

    private static String wrap(String text) {
        return text.length() <= 70 ? text : text.substring(0, 67) + "...";
    }

    private static String color(RelNode node) {
        if (node instanceof NesPhysical.NetworkSink || node instanceof NesPhysical.NetworkSource || node instanceof NesLogical.NetworkSource) {
            return "#fed7aa";
        }
        /// Window operators: aggregations and joins, logical and physical.
        if (node instanceof NesPhysical.WindowBuild || node instanceof NesPhysical.WindowProbe || node instanceof NesWindowAggregate
            || node instanceof NesPhysical.JoinBuild || node instanceof NesPhysical.JoinProbe || node instanceof NesWindowJoin) {
            return "#bbf7d0";
        }
        if (node instanceof NesPhysical.NesRel) {
            return "#fde68a";
        }
        return "#dbeafe";
    }

    private static String escape(String text) {
        return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }
}
