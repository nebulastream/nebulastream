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

    /** Reads one physical source of a logical source; it runs on the source's worker. */
    static final class PhysicalSourceScan extends AbstractRelNode {
        final RelOptTable table;
        final PhysicalSource physicalSource;

        PhysicalSourceScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, PhysicalSource physicalSource) {
            super(cluster, traits);
            this.table = table;
            this.physicalSource = physicalSource;
            this.rowType = table.getRowType();
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
            return super.explainTerms(pw).item("table", table.getQualifiedName()).item("physicalSource", physicalSource.getId());
        }
    }

    /** The rows that another worker sends over a network channel: the receiving end of a cut between two workers. */
    static final class NetworkSource extends AbstractRelNode {
        final String channel;
        final String upstreamHost;

        NetworkSource(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, String channel, String upstreamHost) {
            super(cluster, traits);
            this.rowType = rowType;
            this.channel = channel;
            this.upstreamHost = upstreamHost;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new NetworkSource(getCluster(), traitSet, rowType, channel, upstreamHost);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("from", upstreamHost).item("channel", channel);
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
        return plan.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                Planner.SourceTable table = scan.getTable().unwrapOrThrow(Planner.SourceTable.class);
                if (table.physicalSources().isEmpty()) {
                    throw new UnsupportedOperationException("logical source " + table.name() + " has no physical source");
                }
                List<RelNode> scans = table.physicalSources().stream()
                    .map(source -> (RelNode) new PhysicalSourceScan(scan.getCluster(), scan.getTraitSet(), scan.getTable(), source))
                    .toList();
                return scans.size() == 1 ? scans.get(0) : LogicalUnion.create(scans, true);
            }
        });
    }
}
