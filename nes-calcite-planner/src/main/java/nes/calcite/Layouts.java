package nes.calcite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import nes.calcite.proto.Layout;
import nes.calcite.proto.LayoutPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * The memory layout phase, after placement and pipelining: gives each edge between pipelines a layout (Layout), which
 * its writers (Emits) write and its readers (Scans) read.
 *
 * <p>An edge is a set of operators whose buffers must have one layout: a Scan with the Emits or the source it reads; a
 * Sink or NetworkSink with the Emits that write into it; and, across workers, a NetworkSink with the NetworkSources of
 * its channel. Where a source or the sink is on the edge, it fixes the layout to its format (each has a single one);
 * a native sink (e.g. Void) takes rows. Every other edge gets the layout of the policy.
 *
 * <p>The state of windows and joins has its own layout inside the operators; it is not an edge.
 */
final class Layouts {
    private Layouts() {}

    /** sinkFormat: the sink's format (PlanRequest.sink_format), empty or NATIVE for a native sink. */
    static void assign(List<Placement.Fragment> fragments, String sinkFormat, LayoutPolicy policy) {
        Edges edges = new Edges();
        Map<String, NesPhysical.NetworkSink> senders = new HashMap<>();
        List<NesPhysical.NetworkSource> receivers = new ArrayList<>();
        for (Placement.Fragment fragment : fragments) {
            edges.collect(fragment.root(), senders, receivers);
        }
        for (NesPhysical.NetworkSource receiver : receivers) {
            NesPhysical.NetworkSink sender = senders.get(receiver.channel);
            if (sender != null) {
                edges.union(receiver, sender);
            }
        }
        for (List<RelNode> edge : edges.groups()) {
            Layout layout = layout(edge, sinkFormat, policy);
            for (RelNode node : edge) {
                if (node instanceof NesPhysical.Scan scan) {
                    scan.layout = layout;
                } else if (node instanceof NesPhysical.Emit emit) {
                    emit.layout = layout;
                } else if (node instanceof NesPhysical.NetworkSink sink) {
                    sink.layout = layout;
                }
            }
        }
    }

    private static Layout layout(List<RelNode> edge, String sinkFormat, LayoutPolicy policy) {
        int width = edge.get(0).getRowType().getFieldCount();
        for (RelNode node : edge) {
            if (node instanceof NesPhysical.SourceScan source) {
                return format(source.anonymous != null ? source.anonymous.getFormatOptionsOrDefault("TYPE", "")
                    : source.physicalSource.getFormat());
            }
        }
        boolean native_ = sinkFormat.isEmpty() || sinkFormat.equalsIgnoreCase("NATIVE");
        for (RelNode node : edge) {
            if (node instanceof NesPhysical.Sink) {
                return native_ ? group(Layout.FieldGroup.Kind.ROW, width) : format(sinkFormat);
            }
        }
        return group(policy == LayoutPolicy.COLUMNS ? Layout.FieldGroup.Kind.COLUMN : Layout.FieldGroup.Kind.ROW, width);
    }

    /** A source's or the sink's format, in its own field order. */
    private static Layout format(String type) {
        return Layout.newBuilder().setFormat(Layout.Format.newBuilder().setType(type)).build();
    }

    /** All fields of the row in one group, in the row's order. */
    private static Layout group(Layout.FieldGroup.Kind kind, int width) {
        return Layout.newBuilder().setNative(Layout.Native.newBuilder().addGroups(Layout.FieldGroup.newBuilder().setKind(kind)
            .addAllFields(IntStream.range(0, width).boxed().toList()))).build();
    }

    /** For people (EXPLAIN): e.g. ROW(ID, VAL), COLUMN(ID) COLUMN(VAL), CSV; fields are named after the row. */
    static String describe(Layout layout, RelDataType row) {
        if (layout.hasFormat()) {
            return layout.getFormat().getType().isEmpty() ? "FORMAT" : layout.getFormat().getType();
        }
        List<String> names = row.getFieldNames();
        List<String> groups = new ArrayList<>();
        for (Layout.FieldGroup group : layout.getNative().getGroupsList()) {
            groups.add(group.getKind() + "(" + String.join(", ", group.getFieldsList().stream()
                .map(field -> field < names.size() ? names.get(field) : "$" + field).toList()) + ")");
        }
        return String.join(" ", groups);
    }

    /** The edges: union-find over the operators that write or read the buffers of one edge. */
    private static final class Edges {
        private final Map<RelNode, RelNode> parent = new IdentityHashMap<>();

        void collect(RelNode original, Map<String, NesPhysical.NetworkSink> senders, List<NesPhysical.NetworkSource> receivers) {
            RelNode node = Pipelines.strip(original);
            if (parent.containsKey(node)) {
                return;
            }
            parent.put(node, node);
            if (node instanceof NesPhysical.NetworkSink sink) {
                senders.put(sink.channel, sink);
            } else if (node instanceof NesPhysical.NetworkSource source) {
                receivers.add(source);
            }
            /// A pipeline's start reads the buffers its inputs (Emits, sources) write; a sink takes those of its inputs.
            boolean edge = node instanceof NesPhysical.Scan || node instanceof NesPhysical.Sink || node instanceof NesPhysical.NetworkSink;
            for (RelNode input : node.getInputs()) {
                collect(input, senders, receivers);
                if (edge) {
                    union(node, Pipelines.strip(input));
                }
            }
        }

        RelNode find(RelNode node) {
            RelNode root = node;
            while (parent.get(root) != root) {
                root = parent.get(root);
            }
            parent.put(node, root);
            return root;
        }

        void union(RelNode a, RelNode b) {
            parent.put(find(a), find(b));
        }

        /** The edges with an operator that has a layout (a Scan or an Emit). */
        List<List<RelNode>> groups() {
            Map<RelNode, List<RelNode>> groups = new LinkedHashMap<>();
            for (RelNode node : new ArrayList<>(parent.keySet())) {
                groups.computeIfAbsent(find(node), root -> new ArrayList<>()).add(node);
            }
            return groups.values().stream()
                .filter(group -> group.stream().anyMatch(node -> node instanceof NesPhysical.Scan || node instanceof NesPhysical.Emit))
                .toList();
        }
    }
}
