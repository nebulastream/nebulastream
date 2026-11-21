package nes.calcite.physical;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import nes.calcite.logical.AnonymousSources;
import nes.calcite.logical.NesLogical;
import nes.calcite.logical.NesModelInference;
import nes.calcite.logical.NesWindowJoin;
import nes.calcite.proto.PhysicalOperator;
import nes.calcite.proto.PhysicalSource;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import nes.calcite.physical.NesPhysical.SourceScan;
import nes.calcite.physical.NesPhysical.Scan;
import nes.calcite.physical.NesPhysical.Selection;
import nes.calcite.physical.NesPhysical.InferModel;
import nes.calcite.physical.NesPhysical.JoinState;
import nes.calcite.physical.NesPhysical.JoinBuild;
import nes.calcite.physical.NesPhysical.JoinProbe;
import nes.calcite.physical.NesPhysical.Emit;
import nes.calcite.physical.NesPhysical.Sink;
import nes.calcite.physical.NesPhysical.NetworkSink;
import nes.calcite.physical.NesPhysical.Parts;
import nes.calcite.physical.NesPhysical.NetworkSource;
import nes.calcite.physical.NesPhysical.WindowState;
import nes.calcite.physical.NesPhysical.WatermarkAssigner;
import nes.calcite.physical.NesPhysical.WindowBuild;
import nes.calcite.physical.NesPhysical.WindowProbe;
import nes.calcite.physical.NesPhysical.Retain;
import nes.calcite.physical.NesPhysical.UnionAll;
import static nes.calcite.physical.NesPhysical.NES;
import static nes.calcite.physical.NesPhysical.rename;
import static nes.calcite.physical.NesPhysical.GLOBAL_WINDOW_SIZE_MS;

/**
 * The conversion of the optimized logical plan into NebulaStream's physical operators (NesPhysical): a converter rule per
 * logical operator, which the VolcanoPlanner uses to choose the physical operators of each placed part.
 */
public final class PhysicalRules {
    private PhysicalRules() {}

    /** The converter rules from Calcite's logical operators. */
    public static final List<RelOptRule> RULES = List.of(
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
     * The input of a window operator, with watermarks from the event time in timeField. Usually the rows have them already:
     * from the assigners after their sources (Watermarks.assign), or from another window operator (an aggregation or join),
     * whose watermarks are the ends of its windows; its rows' event times are not in order, so an assigner would raise the
     * watermark too early. Otherwise (e.g. a GLOBAL window, which reads a computed time) the assigner is added here.
     */
    public static RelNode watermarked(RelOptCluster cluster, RelTraitSet traits, RelNode input, int timeField) {
        RelNode converted = ConverterRule.convert(input, traits.replace(NES));
        return watermarked(input) ? converted : new WatermarkAssigner(cluster, traits, converted, timeField);
    }

    /**
     * Whether the rows carry watermarks: from an assigner, a window operator, or (on another worker) over a network channel,
     * through filters, projections and unions whose inputs all carry them.
     */
    public static boolean watermarked(RelNode node) {
        if (node instanceof org.apache.calcite.plan.volcano.RelSubset subset) {
            /// Equivalent alternatives: one of them carries watermarks, all do.
            return subset.getRelList().stream().anyMatch(alternative -> !(alternative instanceof org.apache.calcite.plan.volcano.RelSubset)
                && watermarked(alternative));
        }
        RelNode rel = node instanceof org.apache.calcite.plan.hep.HepRelVertex vertex ? vertex.getCurrentRel() : node;
        if (rel instanceof NesLogical.WindowAggregation || rel instanceof NesWindowJoin || rel instanceof NesLogical.WatermarkAssigner) {
            return true;
        }
        if (rel instanceof NesLogical.NetworkSource network) {
            return network.watermarked;
        }
        boolean passesRows = rel instanceof org.apache.calcite.rel.core.Filter || rel instanceof org.apache.calcite.rel.core.Project
            || rel instanceof org.apache.calcite.rel.core.Union || rel instanceof NesModelInference;
        return passesRows && !rel.getInputs().isEmpty() && rel.getInputs().stream().allMatch(PhysicalRules::watermarked);
    }

    /** A node of the plan that no rule of RULES converts, or null. */
    public static RelNode unsupported(RelNode plan) {
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
            || plan instanceof NesLogical.WindowAggregation;
        return converted ? null : plan;
    }

    /** What a node that no rule converts is, for the error. */
    public static String describeUnsupported(RelNode node) {
        String what = node instanceof org.apache.calcite.rel.core.Values ? "constant rows (VALUES)"
            : node instanceof LogicalUnion ? "UNION without ALL"
            : node.getRelTypeName();
        String terms = org.apache.calcite.plan.RelOptUtil.toString(node).lines().findFirst().orElse("").trim();
        return what + ": " + (terms.length() <= 300 ? terms : terms.substring(0, 300) + "...");
    }

    /** The names given to computed fields in one plan (see Planner, which resets them per plan). */
    private static final ThreadLocal<Set<String>> FRESH_NAMES = ThreadLocal.withInitial(HashSet::new);

    public static void resetFreshNames() {
        FRESH_NAMES.get().clear();
    }

    /**
     * The name of a computed field: its logical name, without the $ of Calcite's generated names (e.g. $f1, EXPR$0),
     * as NebulaStream qualifies names with $. A suffix (_2, _3, ...) makes it unique among the fields of its row and the
     * names given to computed fields in this plan (both compared without case).
     */
    static String freshName(String wanted, java.util.Collection<String> row) {
        String base = wanted.replace('$', '_').replaceAll("^_+", "");
        if (base.isEmpty()) {
            base = "EXPR";
        }
        Set<String> given = FRESH_NAMES.get();
        java.util.function.Predicate<String> taken = name -> given.contains(name.toUpperCase(java.util.Locale.ROOT))
            || row.stream().anyMatch(name::equalsIgnoreCase);
        String name = base;
        for (int i = 2; taken.test(name); i++) {
            name = base + "_" + i;
        }
        given.add(name.toUpperCase(java.util.Locale.ROOT));
        return name;
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
                List<String> row = new ArrayList<>(input.getRowType().getFieldNames());
                row.addAll(outputFields);
                outputFields.add(freshName(output.getName(), row));
                outputTypes.add(output.getType());
            }
            /// The next operator reads the outputs by position: their names only need to be unique.
            return new InferModel(inference.getCluster(), traits, input, inference.model, inference.inputColumns(), outputFields,
                outputTypes);
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
                        : leftCasts.add(rexBuilder.makeAbstractCast(common, new RexInputRef(l.getIndex(), l.getType()), false),
                            left.getRowType().getFieldNames().get(l.getIndex()) + "_KEY"));
                    RexInputRef rightRef = new RexInputRef(r.getIndex() - leftWidth, r.getType());
                    rightKeys.add(r.getType().equals(common) ? rightRef.getIndex()
                        : rightCasts.add(rexBuilder.makeAbstractCast(common, rightRef, false),
                            right.getRowType().getFieldNames().get(rightRef.getIndex()) + "_KEY"));
                }
            }
            left = leftCasts.map(cluster, traits);
            right = rightCasts.map(cluster, traits);

            /// The stored rows: the join's names for its input columns, then the cast keys.
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

        /// The row type of the input with the given names for its first columns, and for the rest (cast keys) their names
        /// in the input, unless another stored column has that name.
        private static RelDataType stored(RelDataTypeFactory types, RelNode input, List<String> names) {
            List<String> all = new ArrayList<>(names);
            while (all.size() < input.getRowType().getFieldCount()) {
                String name = input.getRowType().getFieldNames().get(all.size());
                all.add(all.stream().anyMatch(name::equalsIgnoreCase) ? freshName(name, all) : name);
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

        /// The position of the value's field in the Map's row; the field gets the name (see freshName).
        int add(RexNode expression, String name) {
            List<String> row = new ArrayList<>(input.getRowType().getFieldNames());
            row.addAll(fields);
            fields.add(freshName(name, row));
            expressions.add(expression);
            return input.getRowType().getFieldCount() + fields.size() - 1;
        }

        RelNode map(RelOptCluster cluster, RelTraitSet traits) {
            return fields.isEmpty() ? input : new NesPhysical.Map(cluster, traits, input, fields, expressions);
        }
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
            SourceScan source = new SourceScan(scan.getCluster(), traits, scan.getTable(), scan.physicalSource);
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
            for (int i = 0; i < project.getProjects().size(); i++) {
                RexNode expression = project.getProjects().get(i);
                if (expression instanceof RexInputRef ref && passedOn.add(ref.getIndex())) {
                    projected.add(ref.getIndex());
                } else {
                    /// Computed (or a field projected twice, which needs a field of its own): all in one Map.
                    projected.add(assignments.add(expression, project.getRowType().getFieldNames().get(i)));
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
                            cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(i, inputFields.get(i).getType()), false),
                            unionFields.get(i).getName()));
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
            .withConversion(NesLogical.WindowAggregation.class, Convention.NONE, NES, "NesWindowRule")
            .withRuleFactory(WindowRule::new);

        WindowRule(Config config) {
            super(config);
        }

        @Override
        public RelNode convert(RelNode rel) {
            NesLogical.WindowAggregation aggregate = (NesLogical.WindowAggregation) rel;
            WindowAggregate spec = aggregate.spec;
            RelOptCluster cluster = aggregate.getCluster();
            RelTraitSet traits = aggregate.getTraitSet().replace(NES);
            RelDataTypeFactory types = cluster.getTypeFactory();
            List<RelDataTypeField> inputFields = aggregate.getInput().getRowType().getFieldList();
            List<RelDataTypeField> outputFields = aggregate.getRowType().getFieldList();
            List<Integer> probeColumns = aggregate.probeColumns;

            /// The probe's row: window start and end, the keys, then each result in the engine's type. The window columns and
            /// keys have the types (and all columns the names) of their output columns.
            RelDataType bigint = types.createSqlType(SqlTypeName.BIGINT);
            RelDataType[] probeTypes = new RelDataType[2 + spec.getGroupKeysCount() + spec.getAggregatesCount()];
            String[] probeNames = new String[probeTypes.length];
            probeNames[0] = "START";
            probeNames[1] = "END";
            probeTypes[0] = probeTypes[1] = bigint; // GLOBAL: never read
            for (int i = 0; i < spec.getGroupKeysCount(); i++) {
                probeNames[2 + i] = inputFields.get(spec.getGroupKeys(i)).getName();
                probeTypes[2 + i] = inputFields.get(spec.getGroupKeys(i)).getType();
            }
            List<Integer> arguments = new ArrayList<>();
            for (int i = 0; i < spec.getAggregatesCount(); i++) {
                WindowAggregate.Aggregate call = spec.getAggregates(i);
                RelDataType argument = call.hasField() ? inputFields.get(call.getField()).getType() : null;
                int probe = 2 + spec.getGroupKeysCount() + i;
                int output = probeColumns.indexOf(probe);
                probeNames[probe] = call.getName();
                probeTypes[probe] = engineType(types, call, argument,
                    output < 0 ? types.createSqlType(SqlTypeName.BIGINT) : outputFields.get(output).getType());
                arguments.add(call.hasField() ? call.getField() : -1);
            }
            for (int output = 0; output < outputFields.size(); output++) {
                int probe = probeColumns.get(output);
                probeNames[probe] = outputFields.get(output).getName();
                if (probe < 2 + spec.getGroupKeysCount()) {
                    probeTypes[probe] = outputFields.get(output).getType();
                }
            }
            RelDataTypeFactory.Builder probeRow = types.builder();
            for (int i = 0; i < probeTypes.length; i++) {
                probeRow.add(probeNames[i], probeTypes[i]);
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
                int probe = probeColumns.get(i);
                RelDataType target = outputFields.get(i).getType();
                if (probeFields.get(probe).getType().equals(target)) {
                    fields.add(probe);
                } else {
                    fields.add(casts.add(
                        cluster.getRexBuilder().makeAbstractCast(target, new RexInputRef(probe, probeFields.get(probe).getType()), false),
                        outputFields.get(i).getName()));
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
