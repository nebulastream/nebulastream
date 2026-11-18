package nes.calcite;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nes.calcite.proto.PhysicalSource;
import nes.calcite.proto.WindowAggregate;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

/**
 * NebulaStream's physical operators as a Calcite convention, one node per engine operator (and two helpers, Retain and
 * UnionAll, that Pipelines.finish resolves). The planner decides everything here, including the pipelines: a pipeline
 * starts at a Scan (or is a SourceScan or the Sink) and ends at an Emit. The field names of each node's row type are the engine's physical field names. NesConverterProvider writes each
 * node as a Substrait extension relation with a PhysicalOperator payload, which the engine instantiates as it is.
 */
final class NesPhysical {
    static final Convention NES = new Convention.Impl("NES", NesRel.class);

    /** The converter rules from Calcite's logical operators. */
    static final List<RelOptRule> RULES = List.of(
        SourceScanRule.CONFIG.toRule(),
        SelectionRule.CONFIG.toRule(),
        ProjectionRule.CONFIG.toRule(),
        UnionRule.CONFIG.toRule(),
        WindowRule.CONFIG.toRule(),
        NetworkSourceRule.CONFIG.toRule(),
        PartsRule.CONFIG.toRule());

    /** Fresh names of computed fields, unique within one plan (see Planner, which resets them per plan). */
    private static final ThreadLocal<int[]> FRESH_NAMES = ThreadLocal.withInitial(() -> new int[1]);

    private NesPhysical() {}

    static void resetFreshNames() {
        FRESH_NAMES.get()[0] = 0;
    }

    static String freshName() {
        return "CALCITE_F" + FRESH_NAMES.get()[0]++;
    }

    /** A physical operator of the NES convention. */
    interface NesRel extends RelNode {}

    /** Reads one physical source; its buffers hold the source's raw data (e.g. CSV text), see Scan. */
    static final class SourceScan extends AbstractRelNode implements NesRel {
        final RelOptTable table;
        final PhysicalSource physicalSource;

        SourceScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, PhysicalSource physicalSource) {
            super(cluster, traits);
            this.table = table;
            this.physicalSource = physicalSource;
            this.rowType = rename(cluster.getTypeFactory(), table.getRowType(), physicalSource.getFieldNamesList());
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new SourceScan(getCluster(), traitSet, table, physicalSource);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("table", table.getQualifiedName()).item("physicalSource", physicalSource.getId());
        }
    }

    /**
     * Starts a pipeline: reads the buffers of its inputs as rows of its row type, field by field in order (which also
     * renames them). After a SourceScan it parses the source's raw data instead. Several inputs, each ending in an Emit
     * of the same layout, would be merged; we only read an Emit's buffers again where a result is shared.
     */
    static final class Scan extends AbstractRelNode implements NesRel {
        private List<RelNode> inputs;

        Scan(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RelDataType rowType) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.rowType = rowType;
        }

        boolean parsesSource() {
            return inputs.size() == 1 && Pipelines.strip(inputs.get(0)) instanceof SourceScan;
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
            return new Scan(getCluster(), traitSet, inputs, rowType);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            return pw.item("fields", rowType.getFieldNames());
        }
    }

    /** Emits the rows of its input for which the predicate is true. */
    static final class Selection extends SingleRel implements NesRel {
        final RexNode condition;

        Selection(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode condition) {
            super(cluster, traits, input);
            this.condition = condition;
        }

        @Override
        protected RelDataType deriveRowType() {
            return input.getRowType();
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Selection(getCluster(), traitSet, inputs.get(0), condition);
        }

        @Override
        public RelNode accept(RexShuttle shuttle) {
            RexNode visited = condition.accept(shuttle);
            return visited == condition ? this : new Selection(getCluster(), traitSet, input, visited);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("condition", condition);
        }
    }

    /** Computes one value into one field, which it appends to the row; all other fields stay. */
    static final class Map extends SingleRel implements NesRel {
        final String field;
        final RexNode expression;

        Map(RelOptCluster cluster, RelTraitSet traits, RelNode input, String field, RexNode expression) {
            super(cluster, traits, input);
            this.field = field;
            this.expression = expression;
        }

        @Override
        protected RelDataType deriveRowType() {
            return getCluster().getTypeFactory().builder().addAll(input.getRowType().getFieldList())
                .add(field, expression.getType()).build();
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Map(getCluster(), traitSet, inputs.get(0), field, expression);
        }

        @Override
        public RelNode accept(RexShuttle shuttle) {
            RexNode visited = expression.accept(shuttle);
            return visited == expression ? this : new Map(getCluster(), traitSet, input, field, visited);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("field", field).item("expression", expression);
        }
    }

    /**
     * Ends a pipeline: writes the given fields of its input, in this order, into buffers. Before a sink, it writes them
     * in the sink's output format.
     */
    static final class Emit extends SingleRel implements NesRel {
        final List<Integer> fields;
        final boolean forSink;

        Emit(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> fields, boolean forSink) {
            super(cluster, traits, input);
            this.fields = List.copyOf(fields);
            this.forSink = forSink;
        }

        @Override
        protected RelDataType deriveRowType() {
            List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
            RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
            fields.forEach(field -> builder.add(inputFields.get(field)));
            return builder.build();
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Emit(getCluster(), traitSet, inputs.get(0), fields, forSink);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("fields", getRowType().getFieldNames()).itemIf("forSink", true, forSink);
        }
    }

    /** The query's sink, under the query's output names: each input (an Emit for the sink) is a pipeline writing into it. */
    static final class Sink extends AbstractRelNode implements NesRel {
        private List<RelNode> inputs;
        final List<String> names;

        Sink(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, List<String> names) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.names = names;
            this.rowType = rename(cluster.getTypeFactory(), inputs.get(0).getRowType(), names);
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
            return new Sink(getCluster(), traitSet, inputs, names);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            return pw.item("names", names);
        }
    }

    /**
     * Sends the buffers of its inputs over a network channel to another worker; see Placement. Like the Sink, each input
     * (an Emit, or a NetworkSource whose buffers it forwards) is a pipeline that writes into it.
     */
    static final class NetworkSink extends AbstractRelNode implements NesRel {
        private List<RelNode> inputs;
        final String channel;
        final String downstreamHost;

        NetworkSink(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, String channel, String downstreamHost) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.channel = channel;
            this.downstreamHost = downstreamHost;
            this.rowType = inputs.get(0).getRowType();
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
            return new NetworkSink(getCluster(), traitSet, inputs, channel, downstreamHost);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            return pw.item("to", downstreamHost).item("channel", channel);
        }
    }

    /** Holds the physical parts of a placed plan; Planner takes them apart again. Not an engine operator. */
    static final class Parts extends AbstractRelNode implements NesRel {
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
    }

    /** Receives the buffers of a network channel from another worker, as rows in the native format; see Placement. */
    static final class NetworkSource extends AbstractRelNode implements NesRel {
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

    /**
     * The state of one window, which its WindowBuilds fill and its WindowProbe reads (in the engine: one operator handler).
     * Builds copied into several pipelines (e.g. one per branch of a union) refer to the same state.
     */
    static final class WindowState {
        final WindowAggregate spec;

        WindowState(WindowAggregate spec) {
            this.spec = spec;
        }
    }

    /** Sets the watermark of each buffer from the event time in the given field. */
    static final class WatermarkAssigner extends SingleRel implements NesRel {
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
            return super.explainTerms(pw).item("time", input.getRowType().getFieldNames().get(timeField));
        }
    }

    /**
     * Ends a pipeline: adds each row to the state of its window. Fields are positions in its input: the event time, the
     * group keys, and per aggregation its argument (-1: none, COUNT(*)). The probe's row type names the keys and results.
     */
    static final class WindowBuild extends SingleRel implements NesRel {
        final WindowState window;
        final int timeField;
        final List<Integer> keyFields;
        final List<Integer> aggregationFields;
        final RelDataType probeRowType;

        WindowBuild(RelOptCluster cluster, RelTraitSet traits, RelNode input, WindowState window, int timeField,
                    List<Integer> keyFields, List<Integer> aggregationFields, RelDataType probeRowType) {
            super(cluster, traits, input);
            this.window = window;
            this.timeField = timeField;
            this.keyFields = List.copyOf(keyFields);
            this.aggregationFields = List.copyOf(aggregationFields);
            this.probeRowType = probeRowType;
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new WindowBuild(getCluster(), traitSet, inputs.get(0), window, timeField, keyFields, aggregationFields, probeRowType);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            List<String> names = input.getRowType().getFieldNames();
            return super.explainTerms(pw).item("window", System.identityHashCode(window) % 1000)
                .item("time", names.get(timeField))
                .item("keys", keyFields.stream().map(names::get).toList())
                .item("arguments", aggregationFields.stream().map(f -> f < 0 ? "*" : names.get(f)).toList());
        }
    }

    /**
     * Starts a pipeline: emits a row per closed window and key, from the state its WindowBuilds (its inputs) filled:
     * window start, window end, keys, aggregation results, in the engine's result types.
     */
    static final class WindowProbe extends AbstractRelNode implements NesRel {
        final WindowState window;
        private List<RelNode> inputs;

        WindowProbe(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, WindowState window, RelDataType rowType) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.window = window;
            this.rowType = rowType;
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
            return new WindowProbe(getCluster(), traitSet, inputs, window, rowType);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            WindowAggregate spec = window.spec;
            return pw.item("window", System.identityHashCode(window) % 1000).item("kind", spec.getKind())
                .item("size", spec.getSizeMs()).item("slide", spec.getSlideMs()).item("fields", rowType.getFieldNames());
        }
    }

    /**
     * Not an engine operator: keeps the given fields of its input, in this order. Pipelines.finish folds it into the
     * Emit that ends the pipeline (only an Emit drops fields).
     */
    static final class Retain extends SingleRel implements NesRel {
        final List<Integer> fields;

        Retain(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> fields) {
            super(cluster, traits, input);
            this.fields = List.copyOf(fields);
        }

        @Override
        protected RelDataType deriveRowType() {
            List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
            RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
            fields.forEach(field -> builder.add(inputFields.get(field)));
            return builder.uniquify().build();
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Retain(getCluster(), traitSet, inputs.get(0), fields);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("fields", fields);
        }
    }

    /**
     * Not an engine operator: the rows of all inputs (UNION ALL, or the physical sources of a logical source).
     * Pipelines.finish copies the operators above it into each input's pipeline, up to the pipeline's Emit, so that
     * every input writes into the next pipeline (e.g. the sink) on its own, without a pipeline that merges them.
     */
    static final class UnionAll extends AbstractRelNode implements NesRel {
        private List<RelNode> inputs;

        UnionAll(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RelDataType rowType) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.rowType = rowType;
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
            return new UnionAll(getCluster(), traitSet, inputs, rowType);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            return pw;
        }
    }

    static RelDataType rename(RelDataTypeFactory typeFactory, RelDataType rowType, List<String> names) {
        return typeFactory.createStructType(rowType.getFieldList().stream().map(RelDataTypeField::getType).toList(), names);
    }

    private static List<Integer> all(RelNode node) {
        List<Integer> fields = new ArrayList<>();
        for (int i = 0; i < node.getRowType().getFieldCount(); i++) {
            fields.add(i);
        }
        return fields;
    }

    /** A physical source (NesLogical.PhysicalSourceScan): its SourceScan, parsed by a Scan. */
    public static final class SourceScanRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesLogical.PhysicalSourceScan.class, Convention.NONE, NES, "NesSourceScanRule")
            .withRuleFactory(SourceScanRule::new);

        SourceScanRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesLogical.PhysicalSourceScan scan = (NesLogical.PhysicalSourceScan) rel;
            RelTraitSet traits = scan.getTraitSet().replace(NES);
            SourceScan source = new SourceScan(scan.getCluster(), traits, scan.table, scan.physicalSource);
            return new Scan(scan.getCluster(), traits, List.of(source), source.getRowType());
        }
    }

    /** The receiving end of a cut (NesLogical.NetworkSource): its rows arrive in the native format, a Scan reads them. */
    public static final class NetworkSourceRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesLogical.NetworkSource.class, Convention.NONE, NES, "NesNetworkSourceRule")
            .withRuleFactory(NetworkSourceRule::new);

        NetworkSourceRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesLogical.NetworkSource source = (NesLogical.NetworkSource) rel;
            RelTraitSet traits = source.getTraitSet().replace(NES);
            RelNode network = new NetworkSource(source.getCluster(), traits, source.getRowType(), source.channel, source.upstreamHost);
            return new Scan(source.getCluster(), traits, List.of(network), source.getRowType());
        }
    }

    /** The parts of a placed plan (NesLogical.Parts), each converted on its own. */
    public static final class PartsRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesLogical.Parts.class, Convention.NONE, NES, "NesPartsRule")
            .withRuleFactory(PartsRule::new);

        PartsRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            RelTraitSet traits = rel.getTraitSet().replace(NES);
            return new Parts(rel.getCluster(), traits, rel.getInputs().stream().map(input -> convert(input, NES)).toList());
        }
    }

    public static final class SelectionRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(LogicalFilter.class, Convention.NONE, NES, "NesSelectionRule")
            .withRuleFactory(SelectionRule::new);

        SelectionRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalFilter filter = (LogicalFilter) rel;
            return new Selection(filter.getCluster(), filter.getTraitSet().replace(NES), convert(filter.getInput(), NES),
                filter.getCondition());
        }
    }

    /**
     * A projection: a Map for each computed value (a field passed on is not copied), then a Retain of the projected
     * fields, which the pipeline's Emit applies (see Pipelines.finish).
     */
    public static final class ProjectionRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(LogicalProject.class, Convention.NONE, NES, "NesProjectionRule")
            .withRuleFactory(ProjectionRule::new);

        ProjectionRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalProject project = (LogicalProject) rel;
            RelOptCluster cluster = project.getCluster();
            RelTraitSet traits = project.getTraitSet().replace(NES);
            RelNode current = convert(project.getInput(), NES);
            List<Integer> projected = new ArrayList<>();
            Set<Integer> passedOn = new HashSet<>();
            for (RexNode expression : project.getProjects()) {
                if (expression instanceof RexInputRef ref && passedOn.add(ref.getIndex())) {
                    projected.add(ref.getIndex());
                } else {
                    /// Computed (or a field projected twice, which needs a field of its own).
                    current = new Map(cluster, traits, current, freshName(), expression);
                    projected.add(current.getRowType().getFieldCount() - 1);
                }
            }
            return new Retain(cluster, traits, current, projected);
        }
    }

    /** UNION ALL: a UnionAll of its inputs, each cast to the union's types where they differ. */
    public static final class UnionRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(LogicalUnion.class, Convention.NONE, NES, "NesUnionRule")
            .withRuleFactory(UnionRule::new);

        UnionRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalUnion union = (LogicalUnion) rel;
            if (!union.all) {
                return null; // UNION (distinct) is an aggregation over UNION ALL, see Planner
            }
            RelOptCluster cluster = union.getCluster();
            RelTraitSet traits = union.getTraitSet().replace(NES);
            List<RelNode> branches = new ArrayList<>();
            List<RelDataTypeField> unionFields = union.getRowType().getFieldList();
            for (RelNode input : union.getInputs()) {
                /// Every input has the union's types: a field of another type (e.g. NOT NULL where the union's is nullable)
                /// is cast into a field of its own first.
                RelNode current = convert(input, NES);
                List<Integer> fields = new ArrayList<>();
                List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
                for (int i = 0; i < inputFields.size(); i++) {
                    RelDataType target = unionFields.get(i).getType();
                    if (inputFields.get(i).getType().equals(target)) {
                        fields.add(i);
                    } else {
                        RexNode cast = cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(i, inputFields.get(i).getType()), false);
                        current = new Map(cluster, traits, current, freshName(), cast);
                        fields.add(current.getRowType().getFieldCount() - 1);
                    }
                }
                branches.add(fields.equals(all(input)) ? current : new Retain(cluster, traits, current, fields));
            }
            return new UnionAll(cluster, traits, branches, union.getRowType());
        }
    }

    /**
     * A windowed aggregation (NesWindowAggregate, TUMBLE or HOP): the watermark assigner, the build that ends its input's
     * pipeline, and the probe that starts the next one. The engine types some results differently from Calcite (an
     * unsigned COUNT, a 64-bit SUM, a DOUBLE AVG); those are cast to Calcite's types with a Map each.
     */
    public static final class WindowRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesWindowAggregate.class, Convention.NONE, NES, "NesWindowRule")
            .withRuleFactory(WindowRule::new);

        WindowRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesWindowAggregate aggregate = (NesWindowAggregate) rel;
            WindowAggregate spec = aggregate.spec;
            if (spec.getKind() == WindowAggregate.Kind.GLOBAL || spec.getEmitOnEmptyInput()) {
                return null; // GLOBAL windows (finite inputs) are not supported in physical plans yet
            }
            RelOptCluster cluster = aggregate.getCluster();
            RelTraitSet traits = aggregate.getTraitSet().replace(NES);
            RelDataTypeFactory types = cluster.getTypeFactory();
            List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();
            List<RelDataTypeField> outputFields = aggregate.getRowType().getFieldList();

            /// The probe's row: window start and end, the keys, then each result in the engine's type.
            RelDataTypeFactory.Builder probeRow = types.builder();
            probeRow.add(outputFields.get(0)).add(outputFields.get(1));
            for (int i = 0; i < spec.getGroupKeysCount(); i++) {
                probeRow.add(outputFields.get(2 + i));
            }
            List<Integer> arguments = new ArrayList<>();
            for (int i = 0; i < spec.getAggregatesCount(); i++) {
                WindowAggregate.Aggregate call = spec.getAggregates(i);
                RelDataType argument = call.hasField() ? inputFields.get(call.getField()).getType() : null;
                probeRow.add(outputFields.get(2 + spec.getGroupKeysCount() + i).getName(),
                    engineType(types, call.getFunction(), argument));
                arguments.add(call.hasField() ? call.getField() : -1);
            }
            RelDataType probeRowType = probeRow.uniquify().build();

            WindowState window = new WindowState(spec);
            RelNode input = convert(aggregate.getInput(), NES);
            RelNode assigner = new WatermarkAssigner(cluster, traits, input, spec.getTimeField());
            RelNode build = new WindowBuild(cluster, traits, assigner, window, spec.getTimeField(),
                spec.getGroupKeysList(), arguments, probeRowType);
            RelNode current = new WindowProbe(cluster, traits, List.of(build), window, probeRowType);

            List<Integer> fields = new ArrayList<>();
            List<RelDataTypeField> probeFields = probeRowType.getFieldList();
            for (int i = 0; i < outputFields.size(); i++) {
                RelDataType target = outputFields.get(i).getType();
                if (probeFields.get(i).getType().equals(target)) {
                    fields.add(i);
                } else {
                    RexNode cast = cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(i, probeFields.get(i).getType()), false);
                    current = new Map(cluster, traits, current, freshName(), cast);
                    fields.add(current.getRowType().getFieldCount() - 1);
                }
            }
            return fields.equals(all(current)) ? current : new Retain(cluster, traits, current, fields);
        }

        /// The engine's result type of an aggregation (see its *AggregationLogicalFunction).
        private static RelDataType engineType(RelDataTypeFactory types, WindowAggregate.Function function, RelDataType argument) {
            boolean nullable = argument != null && argument.isNullable();
            RelDataType type = switch (function) {
                case COUNT -> types.createSqlType(SqlTypeName.UBIGINT);
                case AVG -> types.createSqlType(SqlTypeName.DOUBLE);
                case MIN, MAX -> argument;
                case SUM -> switch (argument.getSqlTypeName()) {
                    case TINYINT, SMALLINT, INTEGER, BIGINT -> types.createSqlType(SqlTypeName.BIGINT);
                    case UTINYINT, USMALLINT, UINTEGER, UBIGINT -> types.createSqlType(SqlTypeName.UBIGINT);
                    default -> argument;
                };
                default -> throw new UnsupportedOperationException("window aggregation " + function);
            };
            return types.createTypeWithNullability(type, function != WindowAggregate.Function.COUNT && nullable);
        }
    }
}
