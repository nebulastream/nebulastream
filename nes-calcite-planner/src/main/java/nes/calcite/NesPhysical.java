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
final class NesPhysical {
    static final Convention NES = new Convention.Impl("NES", NesRel.class);

    /** The converter rules from Calcite's logical operators. */
    static final List<RelOptRule> RULES = List.of(
        SourceScanRule.CONFIG.toRule(),
        AnonymousSourceRule.CONFIG.toRule(),
        InferModelRule.CONFIG.toRule(),
        JoinRule.CONFIG.toRule(),
        SelectionRule.CONFIG.toRule(),
        ProjectionRule.CONFIG.toRule(),
        UnionRule.CONFIG.toRule(),
        WindowRule.CONFIG.toRule(),
        NetworkSourceRule.CONFIG.toRule(),
        WatermarkAssignerRule.CONFIG.toRule(),
        PartsRule.CONFIG.toRule());

    /**
     * The size of a GLOBAL window (finite inputs): one tumbling window over the constant event time 0 that holds every row,
     * so only the end of the stream closes it. 10^15 ms are ~30,000 years: far from overflowing window ends. As in the
     * logical converter (SubstraitToLogicalPlan.cpp).
     */
    static final long GLOBAL_WINDOW_SIZE_MS = 1_000_000_000_000_000L;

    /**
     * The input of a window operator, with watermarks from the event time in timeField. Usually the rows have them already:
     * from the assigners after their sources (Watermarks.assign), or from another window operator (an aggregation or join),
     * whose watermarks are the ends of its windows; its rows' event times are not in order, so an assigner would raise the
     * watermark too early. Otherwise (e.g. a GLOBAL window, which reads a computed time) the assigner is added here.
     */
    static RelNode watermarked(RelOptCluster cluster, RelTraitSet traits, RelNode input, int timeField) {
        RelNode converted = ConverterRule.convert(input, traits.replace(NES));
        return watermarked(input) ? converted : new WatermarkAssigner(cluster, traits, converted, timeField);
    }

    /**
     * Whether the rows carry watermarks: from an assigner, a window operator, or (on another worker) over a network channel,
     * through filters, projections and unions whose inputs all carry them.
     */
    static boolean watermarked(RelNode node) {
        if (node instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            /// Equivalent alternatives: one of them carries watermarks, all do.
            return subset.getRelList().stream().anyMatch(alternative -> !(alternative instanceof org.apache.calcite.plan.volcano.RelSubset)
                && watermarked(alternative));
        }
        RelNode rel = node instanceof org.apache.calcite.plan.hep.HepRelVertex vertex ? vertex.getCurrentRel() : node;
        if (rel instanceof NesWindowAggregate || rel instanceof NesWindowJoin || rel instanceof NesLogical.WatermarkAssigner) {
            return true;
        }
        if (rel instanceof NesLogical.NetworkSource network) {
            return network.watermarked;
        }
        boolean passesRows = rel instanceof org.apache.calcite.rel.core.Filter || rel instanceof org.apache.calcite.rel.core.Project
            || rel instanceof org.apache.calcite.rel.core.Union || rel instanceof NesModelInference;
        return passesRows && !rel.getInputs().isEmpty() && rel.getInputs().stream().allMatch(NesPhysical::watermarked);
    }

    /** A node of the plan that no rule of RULES converts, or null. */
    static RelNode unsupported(RelNode plan) {
        for (RelNode input : plan.getInputs()) {
            RelNode found = unsupported(Pipelines.strip(input));
            if (found != null) {
                return found;
            }
        }
        boolean converted = plan instanceof NesLogical.PhysicalSourceScan || plan instanceof AnonymousSources.NesAnonymousSource
            || plan instanceof NesModelInference
            || plan instanceof NesWindowJoin
            || plan instanceof NesLogical.NetworkSource || plan instanceof NesLogical.WatermarkAssigner
            || plan instanceof NesLogical.Parts || plan instanceof LogicalFilter || plan instanceof LogicalProject
            || (plan instanceof LogicalUnion union && union.all)
            || plan instanceof NesWindowAggregate;
        return converted ? null : plan;
    }

    /** What a node that no rule converts is, for the error. */
    static String describeUnsupported(RelNode node) {
        String what = node instanceof org.apache.calcite.rel.core.Values ? "constant rows (VALUES)"
            : node instanceof LogicalUnion ? "UNION without ALL"
            : node.getRelTypeName();
        String terms = org.apache.calcite.plan.RelOptUtil.toString(node).lines().findFirst().orElse("").trim();
        return what + ": " + (terms.length() <= 300 ? terms : terms.substring(0, 300) + "...");
    }

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
    static final class InferModel extends SingleRel implements NesRel {
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
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new InferModel(getCluster(), traitSet, inputs.get(0), model, inputFields, outputFields, outputTypes);
        }

        @Override
        public RelWriter explainTerms(RelWriter pw) {
            return super.explainTerms(pw).item("model", model.getName()).item("reads", inputFields).item("appends", outputFields);
        }
    }

    /** Model inference (NesModelInference): its outputs get fresh fields, appended to the input row. */
    public static final class InferModelRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesModelInference.class, Convention.NONE, NES, "NesInferModelRule")
            .withRuleFactory(InferModelRule::new);

        InferModelRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesModelInference inference = (NesModelInference) rel;
            RelTraitSet traits = inference.getTraitSet().replace(NES);
            RelNode input = convert(inference.getInput(), traits);
            List<RelDataTypeField> fields = inference.getRowType().getFieldList();
            int inputWidth = inference.getInput().getRowType().getFieldCount();
            List<String> outputFields = new ArrayList<>();
            List<RelDataType> outputTypes = new ArrayList<>();
            for (RelDataTypeField output : fields.subList(inputWidth, fields.size())) {
                outputFields.add(freshName());
                outputTypes.add(output.getType());
            }
            /// The next operator reads the outputs by position: their fresh names do not matter.
            return new InferModel(inference.getCluster(), traits, input, inference.model, inference.inputColumns(), outputFields,
                outputTypes);
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
    static final class JoinBuild extends SingleRel implements NesRel {
        final JoinState join;
        final boolean right;
        final int timeField;
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
    static final class JoinProbe extends AbstractRelNode implements NesRel {
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
     * A window join (NesWindowJoin): per side a watermark assigner and a JoinBuild, and a JoinProbe over both. A hash join
     * compares its keys in one type: a key whose side has another type gets a column cast to it (a Map before the build).
     */
    public static final class JoinRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesWindowJoin.class, Convention.NONE, NES, "NesJoinRule")
            .withRuleFactory(JoinRule::new);

        JoinRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesWindowJoin join = (NesWindowJoin) rel;
            RelOptCluster cluster = join.getCluster();
            RelTraitSet traits = join.getTraitSet().replace(NES);
            RexBuilder rexBuilder = cluster.getRexBuilder();
            RelDataTypeFactory types = cluster.getTypeFactory();
            WindowJoin spec = join.spec;
            int leftWidth = join.getLeft().getRowType().getFieldCount();
            boolean hash = spec.getImplementation() == WindowJoin.Implementation.HASH_JOIN;

            RelNode left = watermarked(cluster, traits, join.getLeft(), spec.getLeftTimeField());
            RelNode right = watermarked(cluster, traits, join.getRight(), spec.getRightTimeField());
            Assignments leftCasts = new Assignments(left);
            Assignments rightCasts = new Assignments(right);
            List<Integer> leftKeys = new ArrayList<>();
            List<Integer> rightKeys = new ArrayList<>();
            if (hash) {
                /// The condition is leftField = rightField keys (see WindowJoinRule.split).
                for (RexNode conjunct : RelOptUtil.conjunctions(join.condition)) {
                    List<RexNode> operands = ((RexCall) conjunct).getOperands();
                    RexInputRef a = (RexInputRef) operands.get(0);
                    RexInputRef b = (RexInputRef) operands.get(1);
                    RexInputRef l = a.getIndex() < leftWidth ? a : b;
                    RexInputRef r = a.getIndex() < leftWidth ? b : a;
                    RelDataType common = l.getType().equals(r.getType()) ? l.getType() : types.leastRestrictive(List.of(l.getType(), r.getType()));
                    if (common == null) {
                        throw new UnsupportedOperationException("cannot join keys of types " + l.getType() + " and " + r.getType());
                    }
                    leftKeys.add(l.getType().equals(common) ? l.getIndex()
                        : leftCasts.add(rexBuilder.makeAbstractCast(common, new RexInputRef(l.getIndex(), l.getType()), false)));
                    RexInputRef rightRef = new RexInputRef(r.getIndex() - leftWidth, r.getType());
                    rightKeys.add(r.getType().equals(common) ? rightRef.getIndex()
                        : rightCasts.add(rexBuilder.makeAbstractCast(common, rightRef, false)));
                }
            }
            left = leftCasts.map(cluster, traits);
            right = rightCasts.map(cluster, traits);

            /// The stored rows: the join's names for its input columns, fresh names for the cast keys.
            List<String> names = join.getRowType().getFieldNames();
            RelDataType leftStored = stored(types, left, names.subList(2, 2 + leftWidth));
            RelDataType rightStored = stored(types, right, names.subList(2 + leftWidth, names.size()));
            int leftStoredWidth = leftStored.getFieldCount();
            /// The probe's condition and residual read [left stored row..., right stored row...].
            RexShuttle toStored = new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    return ref.getIndex() < leftWidth ? ref : new RexInputRef(ref.getIndex() - leftWidth + leftStoredWidth, ref.getType());
                }
            };
            RexNode condition;
            List<RexNode> residual = join.residual.stream().map(r -> r.accept(toStored)).toList();
            if (hash) {
                List<RexNode> equalities = new ArrayList<>();
                for (int i = 0; i < leftKeys.size(); i++) {
                    RelDataType type = leftStored.getFieldList().get(leftKeys.get(i)).getType();
                    equalities.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, new RexInputRef(leftKeys.get(i), type),
                        new RexInputRef(leftStoredWidth + rightKeys.get(i), rightStored.getFieldList().get(rightKeys.get(i)).getType())));
                }
                condition = RexUtil.composeConjunction(rexBuilder, equalities);
            } else {
                condition = join.condition == null ? null : join.condition.accept(toStored);
                /// The columns the condition reads, per side (our nested-loop join takes them as its keys).
                java.util.BitSet read = new java.util.BitSet();
                if (condition != null) {
                    read.or(RelOptUtil.InputFinder.bits(condition).toBitSet());
                }
                residual.forEach(r -> read.or(RelOptUtil.InputFinder.bits(r).toBitSet()));
                read.stream().forEach(i -> (i < leftStoredWidth ? leftKeys : rightKeys).add(i < leftStoredWidth ? i : i - leftStoredWidth));
            }

            /// GLOBAL: one window that holds every row (as for window aggregations).
            WindowJoin sized = spec.getKind() == WindowAggregate.Kind.GLOBAL
                ? spec.toBuilder().setSizeMs(GLOBAL_WINDOW_SIZE_MS).setSlideMs(GLOBAL_WINDOW_SIZE_MS).build()
                : spec;
            JoinState state = new JoinState(sized, leftStored, rightStored, leftKeys, rightKeys);
            RelNode leftBuild = new JoinBuild(cluster, traits, left, state, false, spec.getLeftTimeField(), all(left));
            RelNode rightBuild = new JoinBuild(cluster, traits, right, state, true, spec.getRightTimeField(), all(right));
            return new JoinProbe(cluster, traits, List.of(leftBuild, rightBuild), state, condition, residual, join.getRowType());
        }

        /// The row type of the input with the given names for its first columns and fresh ones for the rest (cast keys).
        private static RelDataType stored(RelDataTypeFactory types, RelNode input, List<String> names) {
            List<String> all = new ArrayList<>(names);
            while (all.size() < input.getRowType().getFieldCount()) {
                all.add(freshName());
            }
            return types.createStructType(input.getRowType().getFieldList().stream().map(RelDataTypeField::getType).toList(), all);
        }
    }

    /** The assignments of one Map: each computed value gets a fresh field, appended to the input row. */
    private static final class Assignments {
        private final RelNode input;
        private final List<String> fields = new ArrayList<>();
        private final List<RexNode> expressions = new ArrayList<>();

        Assignments(RelNode input) {
            this.input = input;
        }

        /// The position of the value's field in the Map's row.
        int add(RexNode expression) {
            fields.add(freshName());
            expressions.add(expression);
            return input.getRowType().getFieldCount() + fields.size() - 1;
        }

        RelNode map(RelOptCluster cluster, RelTraitSet traits) {
            return fields.isEmpty() ? input : new Map(cluster, traits, input, fields, expressions);
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

    /** An anonymous source (AnonymousSources.NesAnonymousSource): its SourceScan, parsed by a Scan. */
    public static final class AnonymousSourceRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(AnonymousSources.NesAnonymousSource.class, Convention.NONE, NES, "NesAnonymousSourceRule")
            .withRuleFactory(AnonymousSourceRule::new);

        AnonymousSourceRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            AnonymousSources.NesAnonymousSource anonymous = (AnonymousSources.NesAnonymousSource) rel;
            RelTraitSet traits = anonymous.getTraitSet().replace(NES);
            SourceScan source = new SourceScan(anonymous.getCluster(), traits, anonymous.spec, anonymous.getRowType());
            return new Scan(anonymous.getCluster(), traits, List.of(source), source.getRowType());
        }
    }

    /** The assigner after a source (NesLogical.WatermarkAssigner). */
    public static final class WatermarkAssignerRule extends ConverterRule {
        static final Config CONFIG = Config.INSTANCE
            .withConversion(NesLogical.WatermarkAssigner.class, Convention.NONE, NES, "NesWatermarkAssignerRule")
            .withRuleFactory(WatermarkAssignerRule::new);

        WatermarkAssignerRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesLogical.WatermarkAssigner assigner = (NesLogical.WatermarkAssigner) rel;
            RelTraitSet traits = assigner.getTraitSet().replace(NES);
            return new WatermarkAssigner(assigner.getCluster(), traits, convert(assigner.getInput(), traits), assigner.timeField);
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
            Assignments assignments = new Assignments(convert(project.getInput(), NES));
            List<Integer> projected = new ArrayList<>();
            Set<Integer> passedOn = new HashSet<>();
            for (RexNode expression : project.getProjects()) {
                if (expression instanceof RexInputRef ref && passedOn.add(ref.getIndex())) {
                    projected.add(ref.getIndex());
                } else {
                    /// Computed (or a field projected twice, which needs a field of its own): all in one Map.
                    projected.add(assignments.add(expression));
                }
            }
            return new Retain(cluster, traits, assignments.map(cluster, traits), projected);
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
                Assignments casts = new Assignments(convert(input, NES));
                List<Integer> fields = new ArrayList<>();
                List<RelDataTypeField> inputFields = input.getRowType().getFieldList();
                for (int i = 0; i < inputFields.size(); i++) {
                    RelDataType target = unionFields.get(i).getType();
                    if (inputFields.get(i).getType().equals(target)) {
                        fields.add(i);
                    } else {
                        fields.add(casts.add(
                            cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(i, inputFields.get(i).getType()), false)));
                    }
                }
                RelNode current = casts.map(cluster, traits);
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
                    engineType(types, call, argument, outputFields.get(2 + spec.getGroupKeysCount() + i).getType()));
                arguments.add(call.hasField() ? call.getField() : -1);
            }
            RelDataType probeRowType = probeRow.uniquify().build();

            WindowState window = new WindowState(spec);
            RelNode assigner = watermarked(cluster, traits, aggregate.getInput(), spec.getTimeField());
            RelNode build = new WindowBuild(cluster, traits, assigner, window, spec.getTimeField(),
                spec.getGroupKeysList(), arguments, probeRowType);
            Assignments casts = new Assignments(new WindowProbe(cluster, traits, List.of(build), window, probeRowType));
            List<Integer> fields = new ArrayList<>();
            List<RelDataTypeField> probeFields = probeRowType.getFieldList();
            for (int i = 0; i < outputFields.size(); i++) {
                RelDataType target = outputFields.get(i).getType();
                if (probeFields.get(i).getType().equals(target)) {
                    fields.add(i);
                } else {
                    fields.add(casts.add(
                        cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(i, probeFields.get(i).getType()), false)));
                }
            }
            RelNode current = casts.map(cluster, traits);
            return fields.equals(all(current)) ? current : new Retain(cluster, traits, current, fields);
        }

        /// The engine's result type of an aggregation (see its *AggregationPhysicalFunction). An aggregation of
        /// PlanRequest.functions has its declared type, which is Calcite's (result).
        private static RelDataType engineType(RelDataTypeFactory types, WindowAggregate.Aggregate call, RelDataType argument,
                                              RelDataType result) {
            boolean nullable = argument != null && argument.isNullable();
            RelDataType type = switch (call.getFunction()) {
                case "COUNT" -> types.createSqlType(SqlTypeName.UBIGINT);
                case "AVG" -> types.createSqlType(SqlTypeName.DOUBLE);
                case "MIN", "MAX" -> argument;
                case "SUM" -> switch (argument.getSqlTypeName()) {
                    case TINYINT, SMALLINT, INTEGER, BIGINT -> types.createSqlType(SqlTypeName.BIGINT);
                    case UTINYINT, USMALLINT, UINTEGER, UBIGINT -> types.createSqlType(SqlTypeName.UBIGINT);
                    default -> argument;
                };
                default -> result;
            };
            return types.createTypeWithNullability(type, !call.getFunction().equals("COUNT") && nullable);
        }
    }
}
