package nes.calcite.physical;

import nes.calcite.logical.AnonymousSources;
import nes.calcite.logical.NesLogical;
import nes.calcite.logical.NesModelInference;
import nes.calcite.logical.NesWindowJoin;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nes.calcite.proto.PhysicalOperator;
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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.plan.RelOptUtil;
import nes.calcite.proto.WindowJoin;

/**
 * NebulaStream's physical operators as a Calcite convention, one node per engine operator (and two helpers, Retain and
 * UnionAll, that Pipelines.finish resolves). The planner decides everything here, including the pipelines: a pipeline
 * starts at a Scan (or is a SourceScan or the Sink) and ends at an Emit. The field names of each node's row type are the engine's physical field names. NesConverterProvider writes each
 * node as a Substrait extension relation with a PhysicalOperator payload, which the engine instantiates as it is.
 */
public final class NesPhysical {
    public static final Convention NES = new Convention.Impl("NES", NesRel.class);

    /**
     * The size of a GLOBAL window (finite inputs): one tumbling window over the constant event time 0 that holds every row,
     * so only the end of the stream closes it. 10^15 ms are ~30,000 years: far from overflowing window ends. As in the
     * logical converter (SubstraitToLogicalPlan.cpp).
     */
    static final long GLOBAL_WINDOW_SIZE_MS = 1_000_000_000_000_000L;

    private NesPhysical() {}

    /**
     * A node of the NES convention: an engine operator (NesOperator), or a helper that Pipelines.finish (Retain, UnionAll)
     * or Planner (Parts) resolves.
     */
    public interface NesRel extends RelNode {}

    /** The expression with its field references renumbered: field i is now field fields[i]. */
    static RexNode renumber(RexNode expression, int[] fields) {
        return expression.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return new RexInputRef(fields[ref.getIndex()], ref.getType());
            }
        });
    }

    /** Reads one physical source; its buffers hold the source's raw data (e.g. CSV text), see Scan. */
    static final class SourceScan extends AbstractRelNode implements LayoutSource {
        /// A physical source of the catalog (table and physicalSource), or an anonymous source (anonymous).
        final RelOptTable table;
        final PhysicalSource physicalSource;
        final nes.calcite.proto.AnonymousSource anonymous;

        SourceScan(RelOptCluster cluster, RelTraitSet traits, RelOptTable table, PhysicalSource physicalSource) {
            super(cluster, traits);
            this.table = table;
            this.physicalSource = physicalSource;
            this.anonymous = null;
            this.rowType = rename(cluster.getTypeFactory(), table.getRowType(), physicalSource.getFieldNamesList());
        }

        SourceScan(RelOptCluster cluster, RelTraitSet traits, nes.calcite.proto.AnonymousSource anonymous, RelDataType rowType) {
            super(cluster, traits);
            this.table = null;
            this.physicalSource = null;
            this.anonymous = anonymous;
            this.rowType = rowType;
        }

        @Override
        public boolean startsPipeline() {
            return true;
        }

        /** The source's format: its buffers hold the raw data. */
        @Override
        public nes.calcite.proto.Layout layout(String sinkFormat) {
            return Layouts.format(anonymous != null ? anonymous.getFormatOptionsOrDefault("TYPE", "") : physicalSource.getFormat());
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            PhysicalOperator.SourceScan.Builder scan = operator.getSourceScanBuilder().setOriginId(serialization.newOrigin());
            if (anonymous != null) {
                scan.setAnonymous(anonymous);
            } else {
                scan.setPhysicalSourceId(physicalSource.getId());
            }
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return anonymous != null ? new SourceScan(getCluster(), traitSet, anonymous, rowType)
                : new SourceScan(getCluster(), traitSet, table, physicalSource);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return anonymous != null
                ? super.explainTerms(pw).item("anonymous", anonymous.getType()).item("options", anonymous.getSourceOptionsMap())
                : super.explainTerms(pw).item("table", table.getQualifiedName()).item("physicalSource", physicalSource.getId());
        }
    }

    /**
     * Starts a pipeline: reads the buffers of its inputs in its layout as rows of its row type, field by field in order
     * (which also renames them). After a SourceScan the layout is the source's format, which it parses. Several inputs,
     * each ending in an Emit of the same layout, would be merged; we only read an Emit's buffers again where a result is
     * shared.
     */
    public static final class Scan extends AbstractRelNode implements LaidOut {
        private List<RelNode> inputs;
        /// Set by the memory layout phase (Layouts), after pipelining.
        nes.calcite.proto.Layout layout;

        Scan(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RelDataType rowType) {
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
        public boolean startsPipeline() {
            return true;
        }

        @Override
        public boolean readsInputBuffers() {
            return true;
        }

        @Override
        public nes.calcite.proto.Layout layout() {
            return layout;
        }

        @Override
        public void layOut(nes.calcite.proto.Layout layout) {
            this.layout = layout;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getScanBuilder().setLayout(requireLayout());
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
            return pw.item("fields", rowType.getFieldNames()).itemIf("layout", layout == null ? "" : Layouts.describe(layout, rowType),
                layout != null);
        }
    }

    /** Emits the rows of its input for which the predicate is true. */
    static final class Selection extends SingleRel implements RowOperator {
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
        public NesOperator over(NesOperator input, int[] fields) {
            return new Selection(getCluster(), traitSet, input, renumber(condition, fields));
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            serialization.expression(condition, predicate -> operator.getSelectionBuilder().setPredicate(predicate));
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
    static final class Map extends SingleRel implements RowOperator {
        /// Field i gets expressions i; every expression reads the input row (all are computed before any is written).
        final List<String> fields;
        final List<RexNode> expressions;

        Map(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<String> fields, List<RexNode> expressions) {
            super(cluster, traits, input);
            this.fields = List.copyOf(fields);
            this.expressions = List.copyOf(expressions);
        }

        @Override
        protected RelDataType deriveRowType() {
            RelDataTypeFactory.Builder row = getCluster().getTypeFactory().builder().addAll(input.getRowType().getFieldList());
            for (int i = 0; i < fields.size(); i++) {
                row.add(fields.get(i), expressions.get(i).getType());
            }
            return row.build();
        }

        @Override
        public NesOperator over(NesOperator input, int[] fields) {
            return new Map(getCluster(), traitSet, input, this.fields,
                expressions.stream().map(expression -> renumber(expression, fields)).toList());
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            for (int i = 0; i < fields.size(); i++) {
                PhysicalOperator.Map.Assignment.Builder assignment = operator.getMapBuilder().addAssignmentsBuilder().setField(fields.get(i));
                serialization.expression(expressions.get(i), assignment::setExpression);
            }
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Map(getCluster(), traitSet, inputs.get(0), fields, expressions);
        }

        @Override
        public RelNode accept(RexShuttle shuttle) {
            List<RexNode> visited = expressions.stream().map(expression -> expression.accept(shuttle)).toList();
            return visited.equals(expressions) ? this : new Map(getCluster(), traitSet, input, fields, visited);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            RelWriter writer = super.explainTerms(pw);
            for (int i = 0; i < fields.size(); i++) {
                writer.item(fields.get(i), expressions.get(i));
            }
            return writer;
        }
    }

    /**
     * Runs a model on each row (NesModelInference): reads its inputs from inputFields, in the model's input order, and
     * appends its outputs under the names in outputFields. Stays in its input's pipeline.
     */
    static final class InferModel extends SingleRel implements RowOperator {
        final nes.calcite.proto.Model model;
        final List<Integer> inputFields;
        final List<String> outputFields;
        final List<RelDataType> outputTypes;

        InferModel(RelOptCluster cluster, RelTraitSet traits, RelNode input, nes.calcite.proto.Model model, List<Integer> inputFields,
                   List<String> outputFields, List<RelDataType> outputTypes) {
            super(cluster, traits, input);
            this.model = model;
            this.inputFields = List.copyOf(inputFields);
            this.outputFields = List.copyOf(outputFields);
            this.outputTypes = List.copyOf(outputTypes);
        }

        @Override
        protected RelDataType deriveRowType() {
            RelDataTypeFactory.Builder row = getCluster().getTypeFactory().builder().addAll(input.getRowType().getFieldList());
            for (int i = 0; i < outputFields.size(); i++) {
                row.add(outputFields.get(i), outputTypes.get(i));
            }
            return row.build();
        }

        @Override
        public NesOperator over(NesOperator input, int[] fields) {
            return new InferModel(getCluster(), traitSet, input, model, inputFields.stream().map(field -> fields[field]).toList(),
                outputFields, outputTypes);
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getInferModelBuilder().setModel(model.getName()).addAllInputFields(inputFields);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new InferModel(getCluster(), traitSet, inputs.get(0), model, inputFields, outputFields, outputTypes);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("model", model.getName()).item("reads", inputFields).item("appends", outputFields);
        }
    }

    /**
     * The state of a window join (its handler), shared by the JoinBuilds of both sides and their JoinProbe. Each side's
     * builds store its rows as leftStored / rightStored: the join's input columns, named as the join names them (unique
     * across both sides), and for a hash join the key columns in the type both sides compare in. Keys: positions in the
     * stored rows; for a hash join the keys, pairwise equal, for a nested-loop join the columns the condition reads.
     */
    static final class JoinState {
        final WindowJoin spec;
        final RelDataType leftStored;
        final RelDataType rightStored;
        final List<Integer> leftKeys;
        final List<Integer> rightKeys;

        JoinState(WindowJoin spec, RelDataType leftStored, RelDataType rightStored, List<Integer> leftKeys, List<Integer> rightKeys) {
            this.spec = spec;
            this.leftStored = leftStored;
            this.rightStored = rightStored;
            this.leftKeys = List.copyOf(leftKeys);
            this.rightKeys = List.copyOf(rightKeys);
        }
    }

    /**
     * Ends a pipeline: stores each row of its side (right or left) in the state of its window. Fields are positions in
     * its input: the event time, and the stored fields, which the state names as its side's stored row type.
     */
    static final class JoinBuild extends SingleRel implements NesOperator {
        final JoinState join;
        final boolean right;
        public final int timeField;
        final List<Integer> storedFields;

        JoinBuild(RelOptCluster cluster, RelTraitSet traits, RelNode input, JoinState join, boolean right, int timeField,
                  List<Integer> storedFields) {
            super(cluster, traits, input);
            this.join = join;
            this.right = right;
            this.timeField = timeField;
            this.storedFields = List.copyOf(storedFields);
        }

        @Override
        public NesOperator over(NesOperator input, int[] fields) {
            return new JoinBuild(getCluster(), traitSet, input, join, right, fields[timeField],
                storedFields.stream().map(field -> fields[field]).toList());
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getJoinBuildBuilder().setJoin(serialization.join(join)).setRight(right).setTimeField(timeField)
                .addAllStoredFields(storedFields);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new JoinBuild(getCluster(), traitSet, inputs.get(0), join, right, timeField, storedFields);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            List<String> names = input.getRowType().getFieldNames();
            return super.explainTerms(pw).item("join", System.identityHashCode(join) % 1000).item("side", right ? "right" : "left")
                .item("time", names.get(timeField)).item("stored", storedFields.stream().map(names::get).toList());
        }
    }

    /**
     * Starts a pipeline: emits the joined rows of each closed window, from the state its JoinBuilds (its inputs) filled:
     * window start, window end, the left side's columns, the right side's. The condition and residual read [left stored
     * row..., right stored row...]; for a hash join the condition compares the keys.
     */
    public static final class JoinProbe extends AbstractRelNode implements NesOperator {
        final JoinState join;
        final RexNode condition;
        final List<RexNode> residual;
        private List<RelNode> inputs;

        JoinProbe(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, JoinState join, RexNode condition,
                  List<RexNode> residual, RelDataType rowType) {
            super(cluster, traits);
            this.inputs = new ArrayList<>(inputs);
            this.join = join;
            this.condition = condition;
            this.residual = List.copyOf(residual);
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
        public boolean startsPipeline() {
            return true;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            PhysicalOperator.JoinProbe.Builder probe = operator.getJoinProbeBuilder().setJoin(serialization.join(join))
                .setOutputOriginId(serialization.newOrigin()).addAllInputOriginIds(serialization.inputOrigins());
            if (condition != null) {
                serialization.expression(condition, probe::setCondition);
            }
            residual.forEach(expression -> serialization.expression(expression, probe::addResidual));
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new JoinProbe(getCluster(), traitSet, inputs, join, condition, residual, rowType);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            for (int i = 0; i < inputs.size(); i++) {
                pw.input("input#" + i, inputs.get(i));
            }
            WindowJoin spec = join.spec;
            return pw.item("join", System.identityHashCode(join) % 1000).item("type", spec.getType())
                .item("implementation", spec.getImplementation()).item("kind", spec.getKind()).item("size", spec.getSizeMs())
                .itemIf("condition", condition, condition != null).itemIf("residual", residual, !residual.isEmpty())
                .item("fields", rowType.getFieldNames());
        }
    }

    /**
     * Ends a pipeline: writes the given fields of its input, in this order, into buffers in its layout. Before a sink,
     * the layout is the sink's format.
     */
    public static final class Emit extends SingleRel implements LaidOut {
        final List<Integer> fields;
        /// Set by the memory layout phase (Layouts), after pipelining.
        nes.calcite.proto.Layout layout;

        public Emit(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<Integer> fields) {
            super(cluster, traits, input);
            this.fields = List.copyOf(fields);
        }

        @Override
        protected RelDataType deriveRowType() {
            List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
            RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
            fields.forEach(field -> builder.add(inputFields.get(field)));
            return builder.build();
        }

        @Override
        public NesOperator over(NesOperator input, int[] fields) {
            return new Emit(getCluster(), traitSet, input, this.fields.stream().map(field -> fields[field]).toList());
        }

        @Override
        public nes.calcite.proto.Layout layout() {
            return layout;
        }

        @Override
        public void layOut(nes.calcite.proto.Layout layout) {
            this.layout = layout;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getEmitBuilder().setLayout(requireLayout());
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new Emit(getCluster(), traitSet, inputs.get(0), fields);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("fields", getRowType().getFieldNames())
                .itemIf("layout", layout == null ? "" : Layouts.describe(layout, getRowType()), layout != null);
        }
    }

    /** The query's sink, under the query's output names: each input (an Emit for the sink) is a pipeline writing into it. */
    public static final class Sink extends AbstractRelNode implements LayoutSource {
        private List<RelNode> inputs;
        final List<String> names;

        public Sink(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, List<String> names) {
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
        public boolean startsPipeline() {
            return true;
        }

        @Override
        public boolean readsInputBuffers() {
            return true;
        }

        /** The sink's format; our row layout for NATIVE (or none given). */
        @Override
        public nes.calcite.proto.Layout layout(String sinkFormat) {
            return sinkFormat.isEmpty() || sinkFormat.equalsIgnoreCase("NATIVE")
                ? Layouts.group(nes.calcite.proto.Layout.FieldGroup.Kind.ROW, getRowType().getFieldCount()) : Layouts.format(sinkFormat);
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.setSink(PhysicalOperator.Sink.getDefaultInstance());
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
    public static final class NetworkSink extends AbstractRelNode implements LaidOut, ChannelEnd {
        private List<RelNode> inputs;
        public final String channel;
        final String downstreamHost;
        /// Set by the memory layout phase (Layouts), after pipelining.
        nes.calcite.proto.Layout layout;

        public NetworkSink(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, String channel, String downstreamHost) {
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
        public boolean startsPipeline() {
            return true;
        }

        @Override
        public boolean readsInputBuffers() {
            return true;
        }

        @Override
        public String channel() {
            return channel;
        }

        @Override
        public boolean sends() {
            return true;
        }

        @Override
        public nes.calcite.proto.Layout layout() {
            return layout;
        }

        @Override
        public void layOut(nes.calcite.proto.Layout layout) {
            this.layout = layout;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            serialization.sends(channel);
            operator.getNetworkSinkBuilder().setChannel(channel).setLayout(requireLayout()).setDownstreamHost(downstreamHost);
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
    public static final class Parts extends AbstractRelNode implements NesRel {
        private List<RelNode> inputs;

        public Parts(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
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
    public static final class NetworkSource extends AbstractRelNode implements ChannelEnd {
        public final String channel;
        public final String upstreamHost;

        public NetworkSource(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, String channel, String upstreamHost) {
            super(cluster, traits);
            this.rowType = rowType;
            this.channel = channel;
            this.upstreamHost = upstreamHost;
        }

        @Override
        public boolean startsPipeline() {
            return true;
        }

        @Override
        public String channel() {
            return channel;
        }

        @Override
        public boolean sends() {
            return false;
        }

        /** The rows keep the origins (and sequence numbers and watermarks) they had on the sending worker. */
        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getNetworkSourceBuilder().setChannel(channel).setUpstreamHost(upstreamHost)
                .setOriginId(serialization.receives(channel).get(0));
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
    public static final class WatermarkAssigner extends SingleRel implements NesOperator {
        public final int timeField;

        public WatermarkAssigner(RelOptCluster cluster, RelTraitSet traits, RelNode input, int timeField) {
            super(cluster, traits, input);
            this.timeField = timeField;
        }

        /**
         * As early in the pipeline as the time field allows: below operators that keep the row (RowOperator), so that the
         * watermark also advances with rows that a Selection drops, unless one computes the time field.
         */
        @Override
        public NesOperator over(NesOperator input, int[] fields) {
            return earliest(input, fields[timeField]);
        }

        private NesOperator earliest(NesOperator node, int timeField) {
            if (node instanceof RowOperator && timeField < node.getInput(0).getRowType().getFieldCount()) {
                return (NesOperator) node.copy(node.getTraitSet(), List.of(earliest((NesOperator) node.getInput(0), timeField)));
            }
            return new WatermarkAssigner(getCluster(), traitSet, node, timeField);
        }

        /** The row of its input: an assigner below others (see over) leaves their fields where they are. */
        @Override
        public int[] fields(int[] inputFields, NesOperator copy) {
            return inputFields;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getWatermarkAssignerBuilder().setTimeField(timeField);
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
    static final class WindowBuild extends SingleRel implements NesOperator {
        final WindowState window;
        public final int timeField;
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
        public NesOperator over(NesOperator input, int[] fields) {
            return new WindowBuild(getCluster(), traitSet, input, window, fields[timeField],
                keyFields.stream().map(field -> fields[field]).toList(),
                aggregationFields.stream().map(field -> field < 0 ? field : fields[field]).toList(), probeRowType);
        }

        /** Its aggregations: per aggregation the function and its argument; COUNT(*) counts its time field, NULLs included. */
        List<PhysicalOperator.WindowAggregation> aggregations() {
            List<PhysicalOperator.WindowAggregation> result = new ArrayList<>();
            for (int i = 0; i < aggregationFields.size(); i++) {
                PhysicalOperator.WindowAggregation.Builder aggregation = PhysicalOperator.WindowAggregation.newBuilder()
                    .setFunction(window.spec.getAggregates(i).getFunction());
                if (aggregationFields.get(i) >= 0) {
                    aggregation.setField(aggregationFields.get(i));
                } else {
                    aggregation.setField(timeField).setIncludeNulls(true);
                }
                result.add(aggregation.build());
            }
            return result;
        }

        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            operator.getWindowBuildBuilder().setWindow(serialization.window(window)).setTimeField(timeField).addAllKeyFields(keyFields)
                .addAllAggregations(aggregations()).addAllProbeSchema(serialization.fields(probeRowType));
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
    public static final class WindowProbe extends AbstractRelNode implements NesOperator {
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
        public boolean startsPipeline() {
            return true;
        }

        /** The aggregations as in its first build, whose input the engine reads the argument types from. */
        @Override
        public void describe(PhysicalOperator.Builder operator, Serialization serialization) {
            WindowBuild first = (WindowBuild) Pipelines.strip(inputs.get(0));
            operator.getWindowProbeBuilder().setWindow(serialization.window(window)).setOutputOriginId(serialization.newOrigin())
                .addAllInputOriginIds(serialization.inputOrigins()).addAllAggregations(first.aggregations());
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

}
