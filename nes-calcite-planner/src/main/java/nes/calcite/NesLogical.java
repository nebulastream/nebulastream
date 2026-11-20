package nes.calcite;

import java.util.ArrayList;
import java.util.List;
import nes.calcite.proto.PhysicalSource;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;

/**
 * Logical operators for placement (Placement), in Calcite's logical convention: the plan is placed before it becomes
 * physical operators, operator by operator, as our own optimizer places logical operators.
 */
final class NesLogical {
    private NesLogical() {}

    /**
     * Reads one physical source of a logical source; it runs on the source's worker. A TableScan of the logical source's
     * table, so that Calcite's rules and metadata handle it as a table (e.g. RelFieldTrimmer trims its unused columns,
     * column origins find its columns).
     */
    static final class PhysicalSourceScan extends TableScan {
        final PhysicalSource physicalSource;

        PhysicalSourceScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, PhysicalSource physicalSource) {
            super(cluster, traits, List.of(), table);
            this.physicalSource = physicalSource;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new PhysicalSourceScan(getCluster(), traitSet, table, physicalSource);
        }

        @Override
        public double estimateRowCount(org.apache.calcite.rel.metadata.RelMetadataQuery mq) {
            return 100;
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("physicalSource", physicalSource.getId());
        }
    }

    /**
     * Sets the watermarks of the rows of one source from their event time in timeField. It is the first operator after
     * the source (Watermarks.assign), so it runs on the source's worker (Placement) and every row of the source, also one
     * that a later filter drops, advances the watermark.
     */
    static final class WatermarkAssigner extends org.apache.calcite.rel.SingleRel {
        final int timeField;

        WatermarkAssigner(RelOptCluster cluster, RelTraitSet traits, RelNode input, int timeField) {
            super(cluster, traits, input);
            this.timeField = timeField;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new WatermarkAssigner(getCluster(), traitSet, inputs.get(0), timeField);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("time", getRowType().getFieldNames().get(timeField));
        }
    }

    /** The rows that another worker sends over a network channel: the receiving end of a cut between two workers. */
    static final class NetworkSource extends AbstractRelNode {
        final String channel;
        final String upstreamHost;
        /// Whether the sent rows carry watermarks (from an assigner after their sources or from a window operator), which
        /// must not be reassigned.
        final boolean watermarked;

        NetworkSource(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, String channel, String upstreamHost,
            boolean watermarked) {
            super(cluster, traits);
            this.rowType = rowType;
            this.channel = channel;
            this.upstreamHost = upstreamHost;
            this.watermarked = watermarked;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new NetworkSource(getCluster(), traitSet, rowType, channel, upstreamHost, watermarked);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("from", upstreamHost).item("channel", channel).itemIf("watermarked", true, watermarked);
        }
    }

    /** Holds the parts of a placed plan, so that the planner turns all of them into physical operators at once. */
    static final class Parts extends AbstractRelNode {
        private List<RelNode> inputs;

        Parts(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.rowType = cluster.getTypeFactory().builder().build();
        }

        @Override
        public List<RelNode> getInputs() {
            return inputs;
        }

        @Override
        public void replaceInput(int ordinalInParent, RelNode p) {
            inputs.set(ordinalInParent, p);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Parts(getCluster(), traitSet, inputs);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("part#" + i, inputs.get(i));
            }
            return pw;
        }
    }

    /** Replaces each scan of a logical source by a scan of each of its physical sources (a UNION ALL of several). */
    static RelNode expandSources(RelNode plan) {
        return plan.accept(new org.apache.calcite.rel.RelHomogeneousShuttle() {
            /// Also the tables of subqueries, which are expressions until Calcite removes them.
            private final org.apache.calcite.rex.RexShuttle subqueries = new org.apache.calcite.rex.RexShuttle() {
                @Override
                public org.apache.calcite.rex.RexNode visitSubQuery(org.apache.calcite.rex.RexSubQuery subQuery) {
                    org.apache.calcite.rex.RexSubQuery visited = (org.apache.calcite.rex.RexSubQuery) super.visitSubQuery(subQuery);
                    RelNode expanded = expandSources(visited.rel);
                    return expanded == visited.rel ? visited : visited.clone(expanded);
                }
            };

            @Override
            public RelNode visit(RelNode other) {
                /// Our sources are table scans, too, but already expanded.
                if (other instanceof TableScan scan && !(scan instanceof PhysicalSourceScan)
                    && !(scan instanceof AnonymousSources.NesAnonymousSource)) {
                    return expand(scan);
                }
                return super.visit(other).accept(subqueries);
            }
        });
    }

    private static RelNode expand(TableScan scan) {
        Planner.SourceTable table = scan.getTable().unwrapOrThrow(Planner.SourceTable.class);
        if (table.physicalSources().isEmpty()) {
            throw new UnsupportedOperationException("logical source " + table.name() + " has no physical source");
        }
        List<RelNode> scans = table.physicalSources().stream()
            .map(source -> (RelNode) new PhysicalSourceScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), source))
            .toList();
        return scans.size() == 1 ? scans.get(0) : LogicalUnion.create(scans, true);
    }
}
