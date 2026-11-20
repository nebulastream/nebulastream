package nes.calcite;

import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.plan.Plan;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.Properties;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.Source;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import java.util.ArrayList;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

/** SQL + source schemas -> optimized Calcite plan -> Substrait. Thread-safe. */
public final class Planner {
    /// steps: the plan after each step of the planner, for inspection (PlanResponse.steps).
    public record Result(Plan plan, String explain, List<nes.calcite.proto.PlanStep> steps) {}

    /// Takes over NebulaStream's PredicatePushdownRule, ProjectionPushdownRule and RedundantProjectionRemovalRule
    /// (see SKIPPED_NES_RULES on the C++ side):
    ///   1. trim unused columns: projections right above the sources, unused aggregates dropped (RelFieldTrimmer);
    ///   2. normalize so that windowed aggregations and joins sit directly on TUMBLE/HOP, and rewrite them
    ///      (WindowAggregateRule, WindowJoinRule; the latter also picks the join implementation);
    ///   3. push filters down through projections, unions and window group keys, and drop identity projections.
    /// Unused-column trimming runs first because the trimmer only understands Calcite's own Aggregate.
    private static final HepProgram REMOVE_SUBQUERIES = HepProgram.builder()
        .addRuleCollection(List.of(
            CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
            CoreRules.JOIN_SUB_QUERY_TO_CORRELATE))
        .build();

    /// TABLE(MODEL_INFERENCE(...)) becomes our inference operator, TABLE(FILE(...)) etc. an anonymous source.
    private static final HepProgram MODEL_INFERENCE = HepProgram.builder()
        .addRuleInstance(ModelInference.RULE)
        .addRuleInstance(AnonymousSources.RULE)
        .build();

    private static final HepProgram PREPARE_AND_REWRITE_WINDOWS = HepProgram.builder()
        /// Distinct UNION, INTERSECT and EXCEPT become UNION ALL plus aggregations (Calcite's stock rewrites).
        .addRuleCollection(List.of(CoreRules.UNION_TO_DISTINCT, CoreRules.INTERSECT_TO_DISTINCT, CoreRules.MINUS_TO_DISTINCT))
        /// Those count with COUNT() FILTER (WHERE ...), which our aggregations lack: COUNT(CASE WHEN ... THEN 1 END) instead.
        .addRuleInstance(CoreRules.AGGREGATE_FILTER_TO_CASE)
        /// Our aggregations have no DISTINCT: COUNT(DISTINCT x) becomes a grouping by x, then COUNT(x); several of them
        /// are computed separately and joined (the variant without joins needs GROUPING SETS).
        .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
        /// Join order: Calcite's stock heuristic join reordering, which joins connected tables before cross products.
        .addRuleCollection(List.of(CoreRules.FILTER_INTO_JOIN, CoreRules.JOIN_CONDITION_PUSH))
        .addMatchOrder(org.apache.calcite.plan.hep.HepMatchOrder.BOTTOM_UP)
        .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
        .addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE)
        .addRuleCollection(List.of(
            CoreRules.FILTER_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_REMOVE,
            CoreRules.AGGREGATE_PROJECT_MERGE,
            ProjectWindowTransposeRule.INSTANCE))
        .addRuleInstance(WindowAggregateRule.INSTANCE)
        .addRuleInstance(WindowJoinRule.INSTANCE)
        .build();

    private static final HepProgram GLOBAL_WINDOWS = HepProgram.builder()
        .addRuleInstance(GlobalWindowRules.SINGLE_ROW_OUTER_JOIN)
        .addRuleCollection(List.of(GlobalWindowRules.AGGREGATE, GlobalWindowRules.JOIN))
        .build();

    private static final HepProgram PUSHDOWN = HepProgram.builder()
        .addRuleCollection(List.of(
            CoreRules.FILTER_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            FilterWindowTransposeRule.INSTANCE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_REMOVE))
        .build();

    private static final HepProgram PUSH_INTO_SOURCES = HepProgram.builder()
        .addRuleCollection(List.of(
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.PROJECT_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_MERGE,
            CoreRules.PROJECT_MERGE,
            CoreRules.PROJECT_REMOVE))
        .build();

    private static final HepProgram REMOVE_IDENTITY_PROJECTIONS = HepProgram.builder()
        .addRuleInstance(CoreRules.PROJECT_REMOVE)
        .build();

    /** A converter per set of the engine's functions (the same for all requests of one engine). */
    private final java.util.Map<List<nes.calcite.proto.Function>, NesConverterProvider> providers =
        new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The plan whose own column names are the query's output names, so that the converter maps each relation to one of
     * our operators and decides nothing: projections that pass their input on unchanged are removed, and a projection
     * renames the columns where needed. A name that differs only in case from its column's name is that column's name
     * (SELECT "value" of a column VALUE); a repeated name (SELECT a, a) gets a suffix.
     */
    /**
     * The output projection with the results that cannot be NULL typed so: a cast that only makes a value that is never
     * NULL nullable is left out, COALESCE of a first value that is never NULL is that value, and a CASE whose results are
     * never NULL is not nullable. E.g. the window start of
     * a full outer join (COALESCE(l.window_start, r.window_start)), which Calcite types nullable as either side may be
     * padded, while our join has one window start for both sides that is never NULL.
     */
    private static RelNode nonNullResults(RelNode rel) {
        if (!(rel instanceof org.apache.calcite.rel.core.Project project)) {
            return rel;
        }
        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RexShuttle tighten = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                RexNode visited = super.visitCall(call);
                if (!(visited instanceof RexCall node)) {
                    return visited;
                }
                /// Only a cast to the nullable type: a cast to the same type is explicit for NebulaStream (Unsigned.prepare),
                /// e.g. CHAR_LENGTH, which is UINT64 to us, cast to Calcite's INTEGER.
                if (node.getKind() == SqlKind.CAST && node.getType().isNullable() && !node.getOperands().get(0).getType().isNullable()
                    && org.apache.calcite.sql.type.SqlTypeUtil.equalSansNullability(node.getType(), node.getOperands().get(0).getType())) {
                    return node.getOperands().get(0);
                }
                if (node.getKind() == SqlKind.COALESCE && !node.getOperands().get(0).getType().isNullable()) {
                    return node.getOperands().get(0);
                }
                /// CASE with results that are never NULL (its THEN values and the ELSE value).
                if (node.getKind() == SqlKind.CASE && node.getType().isNullable()) {
                    List<RexNode> operands = node.getOperands();
                    boolean results = true;
                    for (int i = 1; i < operands.size(); i += 2) {
                        results &= !operands.get(i).getType().isNullable();
                    }
                    results &= !operands.get(operands.size() - 1).getType().isNullable();
                    if (results) {
                        return rexBuilder.makeCall(rexBuilder.getTypeFactory().createTypeWithNullability(node.getType(), false),
                            node.getOperator(), operands);
                    }
                }
                return node;
            }
        };
        List<RexNode> projects = project.getProjects().stream().map(e -> e.accept(tighten)).toList();
        return projects.equals(project.getProjects()) ? rel
            : org.apache.calcite.rel.logical.LogicalProject.create(project.getInput(), List.of(), projects, project.getRowType().getFieldNames(),
                Set.of());
    }

    /**
     * Event-time columns have their source's type again in the output. Calcite's TUMBLE and HOP only accept TIMESTAMP columns,
     * so SourceTable declares each DESCRIPTOR column as TIMESTAMP(3), which our operators store as UINT64. An output column
     * that is such a source column, unchanged, is cast back to the source's type (e.g. INT64), as the logical path types it
     * from the source. Computed columns, e.g. window starts, stay UINT64.
     */
    private static RelRoot sourceTimeTypes(RelRoot root, Steps steps) {
        RelNode rel = root.rel;
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        List<RexNode> columns = new ArrayList<>();
        boolean cast = false;
        for (int i = 0; i < rel.getRowType().getFieldCount(); i++) {
            RelDataType type = rel.getRowType().getFieldList().get(i).getType();
            RexNode column = RexInputRef.of(i, rel.getRowType());
            Field field = type.getSqlTypeName() == SqlTypeName.TIMESTAMP ? sourceColumn(rel, i) : null;
            /// TIMESTAMP is UINT64 to our operators: only other source types need the cast.
            if (field != null && field.getType() != FieldType.TIMESTAMP_MS && field.getType() != FieldType.UINT64) {
                RelDataType sourceType = rexBuilder.getTypeFactory().createTypeWithNullability(
                    relType(rexBuilder.getTypeFactory(), field.getType(), field.getName()), type.isNullable());
                column = rexBuilder.makeAbstractCast(sourceType, column, false);
                cast = true;
            }
            columns.add(column);
        }
        if (!cast) {
            return root;
        }
        return root.withRel(steps.add("event-time columns typed as in their sources", "ours: Planner.sourceTimeTypes",
            org.apache.calcite.rel.logical.LogicalProject.create(rel, List.of(), columns, rel.getRowType().getFieldNames(), Set.of())));
    }

    /**
     * The source column that column i of the relation passes on unchanged, or null: through projections, filters, unions
     * (if every input passes on the same source column) and window joins. Calcite's column origins do not know our nodes.
     */
    private static Field sourceColumn(RelNode node, int i) {
        RelNode rel = node instanceof org.apache.calcite.plan.hep.HepRelVertex vertex ? vertex.getCurrentRel() : node;
        if (rel instanceof org.apache.calcite.rel.core.TableScan scan) {
            SourceTable table = scan.getTable().unwrap(SourceTable.class);
            return table == null ? null : table.source.getFields(i);
        }
        if (rel instanceof NesLogical.PhysicalSourceScan scan) {
            SourceTable table = scan.table.unwrap(SourceTable.class);
            return table == null ? null : table.source.getFields(i);
        }
        if (rel instanceof org.apache.calcite.rel.core.Project project) {
            return project.getProjects().get(i) instanceof RexInputRef ref ? sourceColumn(project.getInput(), ref.getIndex()) : null;
        }
        if (rel instanceof org.apache.calcite.rel.core.Filter filter) {
            return sourceColumn(filter.getInput(), i);
        }
        if (rel instanceof org.apache.calcite.rel.core.Union union) {
            Field first = null;
            for (RelNode input : union.getInputs()) {
                Field field = sourceColumn(input, i);
                if (field == null || (first != null && field.getType() != first.getType())) {
                    return null;
                }
                first = field;
            }
            return first;
        }
        if (rel instanceof NesWindowJoin join) {
            /// START, END, the left input's columns, the right input's columns.
            int left = join.getLeft().getRowType().getFieldCount();
            return i < 2 ? null : i - 2 < left ? sourceColumn(join.getLeft(), i - 2) : sourceColumn(join.getRight(), i - 2 - left);
        }
        return null;
    }

    private static RelRoot outputNames(RelRoot root, RelBuilder relBuilder, Steps steps) {
        RelNode rel = nonNullResults(steps.rules("identity projections removed", "rules: Calcite's, selected by us",
            REMOVE_IDENTITY_PROJECTIONS, root.project()));
        List<String> current = rel.getRowType().getFieldNames();
        org.apache.calcite.rel.core.Project top = rel instanceof org.apache.calcite.rel.core.Project project ? project : null;
        List<String> names = new ArrayList<>();
        for (int i = 0; i < current.size(); i++) {
            /// the name of the column this one passes on
            String column = top != null && top.getProjects().get(i) instanceof RexInputRef ref
                ? top.getInput().getRowType().getFieldNames().get(ref.getIndex())
                : current.get(i);
            String wanted = root.fields.get(i).getValue();
            names.add(wanted.equalsIgnoreCase(column) ? column : wanted);
        }
        names = SqlValidatorUtil.uniquify(names, true);
        if (!names.equals(current)) {
            /// The top projection gets the names, else a projection of its own (RelBuilder.rename would rename the columns
            /// of the relations below instead).
            List<RexNode> columns = new ArrayList<>();
            for (int i = 0; i < names.size(); i++) {
                columns.add(RexInputRef.of(i, rel.getRowType()));
            }
            rel = steps.add("output columns named", "ours: Planner.outputNames", top != null
                ? org.apache.calcite.rel.logical.LogicalProject.create(top.getInput(), List.of(), top.getProjects(), names, Set.of())
                : org.apache.calcite.rel.logical.LogicalProject.create(rel, List.of(), columns, names, Set.of()));
        }
        return RelRoot.of(rel, root.kind);
    }

    /** The engine's functions for plans without a request (tools and tests), as our engine declares them. */
    public static final List<nes.calcite.proto.Function> TEST_FUNCTIONS = List.of(
        nes.calcite.proto.Function.newBuilder().setName("FROM_BASE64").addOperands("STRING").setResult("VARBINARY").build(),
        nes.calcite.proto.Function.newBuilder().setName("MEDIAN").addOperands("NUMERIC").setResult("DOUBLE").setAggregation(true).build());

    public Result plan(String sql, List<Source> sources) throws Exception {
        return plan(sql, sources, false);
    }

    /// finiteInputs: joins and aggregations without a window get a GLOBAL window (see GlobalWindowRules).
    public Result plan(String sql, List<Source> sources, boolean finiteInputs) throws Exception {
        return plan(sql, sources, finiteInputs, false);
    }

    /// physical: plan down to NebulaStream's physical operators (NesPhysical) instead of a logical plan.
    public Result plan(String sql, List<Source> sources, boolean finiteInputs, boolean physical) throws Exception {
        return plan(sql, sources, finiteInputs, physical, List.of(), "", false, List.of(), List.of(), List.of(), false, TEST_FUNCTIONS,
            "", nes.calcite.proto.LayoutPolicy.ROWS);
    }

    /// A request of the service (PlanRequest).
    public Result plan(nes.calcite.proto.PlanRequest request) throws Exception {
        return plan(request.getSql(), request.getSourcesList(), request.getFiniteInputs(), request.getPhysical(),
            request.getTopologyList(), request.getSinkHost(), request.getDetailedSteps(), request.getModelsList(),
            request.getSourceTypesList(), request.getFormatOptionsList(), request.getNestedLoopJoins(), request.getFunctionsList(),
            request.getSinkFormat(), request.getLayoutPolicy());
    }

    /// topology: place the physical plan on these workers, with the sink on sinkHost (see Placement).
    /// detailedSteps: a graph per step, and the plan after each rule (see Steps). models: for MODEL_INFERENCE.
    /// nestedLoopJoins: physical plans join with nested loops only (see PlanRequest.nested_loop_joins).
    /// sinkFormat, layoutPolicy: the sink's format and the policy for the buffers between pipelines (see Layouts).
    public Result plan(String sql, List<Source> sources, boolean finiteInputs, boolean physical,
                       List<nes.calcite.proto.Worker> topology, String sinkHost, boolean detailedSteps,
                       List<nes.calcite.proto.Model> models, List<nes.calcite.proto.SourceType> sourceTypes,
                       List<String> formatOptions, boolean nestedLoopJoins, List<nes.calcite.proto.Function> functions,
                       String sinkFormat, nes.calcite.proto.LayoutPolicy layoutPolicy) throws Exception {
        NesConverterProvider provider = providers.computeIfAbsent(List.copyOf(functions), NesConverterProvider::new);
        SqlParser.Config parserConfig = provider.getSqlParserConfig()
            /// As in standard SQL and our own identifiers: unquoted names are upper case, quoted ones keep their case, so the
            /// names Calcite returns tell them apart (see toIdentifier in SubstraitToLogicalPlan.cpp). Lookups ignore case.
            .withUnquotedCasing(Casing.TO_UPPER)
            .withQuotedCasing(Casing.UNCHANGED)
            .withCaseSensitive(false);
        SqlNode parsed = SqlParser.create(sql, parserConfig).parseQuery();
        if (!parsed.isA(SqlKind.QUERY)) {
            throw new UnsupportedOperationException("Only queries are supported, got " + parsed.getKind());
        }

        RelDataTypeFactory typeFactory = provider.typeFactory();
        parsed = NumericLiterals.check(parsed);
        parsed = WindowInputs.wrapSubqueries(parsed);
        Set<String> eventTimeFields = descriptorColumns(parsed);
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        for (Source source : sources) {
            schema.add(source.getName(), new SourceTable(source, eventTimeFields));
        }
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
        CalciteCatalogReader catalog =
            new CalciteCatalogReader(schema, List.of(), typeFactory, new CalciteConnectionConfigImpl(properties));

        SqlValidator validator = SqlValidatorUtil.newValidator(
            org.apache.calcite.sql.util.SqlOperatorTables.chain(provider.getSqlOperatorTable(),
                org.apache.calcite.sql.util.SqlOperatorTables.of(new ModelInference.Function(models)),
                org.apache.calcite.sql.util.SqlOperatorTables.of(AnonymousSources.functions(sourceTypes, formatOptions))),
            catalog, typeFactory, SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validated = validator.validate(parsed);

        /// The rule programs run in their own HepPlanners; the cluster's VolcanoPlanner only chooses physical operators.
        VolcanoPlanner volcano = new VolcanoPlanner();
        volcano.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(volcano, new RexBuilder(typeFactory));
        cluster.setMetadataQuerySupplier(() -> new RelMetadataQuery(CachingMetadataHandlerProvider.INSTANCE));
        cluster.getPlanner().setExecutor(InterpretingRexExecutor.INSTANCE);
        SqlToRelConverter converter = new SqlToRelConverter(
            null, validator, catalog, cluster, StandardConvertletTable.INSTANCE, provider.getSqlToRelConverterConfig());
        RelRoot root = converter.convertQuery(validated, false, true);

        /// IN/EXISTS/scalar subqueries and LATERAL become (semi/anti) joins and aggregations.
        RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, catalog);
        Steps steps = new Steps(detailedSteps);
        RelNode optimized = steps.add("converted from SQL", "Calcite: SqlToRelConverter", root.rel);
        if (physical) {
            /// Physical plans read physical sources: all later steps (e.g. the pushdown of filters) see each of them.
            optimized = steps.add("sources expanded per physical source", "ours: NesLogical.expandSources", NesLogical.expandSources(optimized));
        }
        optimized = steps.rules("subqueries removed", "rules: Calcite's, selected by us", REMOVE_SUBQUERIES, optimized);
        /// After the subqueries are removed: rules do not look into subqueries, which may call table functions, too.
        optimized = steps.rules("table functions rewritten (MODEL_INFERENCE, anonymous sources)",
            "rules: ours (ModelInference, AnonymousSources)", MODEL_INFERENCE, optimized);
        optimized = steps.add("decorrelated", "Calcite: RelDecorrelator", RelDecorrelator.decorrelateQuery(optimized, relBuilder));
        optimized = steps.add("unused fields trimmed", "Calcite: RelFieldTrimmer", new RelFieldTrimmer(validator, relBuilder).trim(optimized));
        optimized = steps.rules("windows rewritten", "rules: Calcite's and ours, selected by us", PREPARE_AND_REWRITE_WINDOWS, optimized);
        if (finiteInputs) {
            optimized = steps.rules("global windows for finite inputs", "rules: ours (GlobalWindowRules)", GLOBAL_WINDOWS, optimized);
        }
        optimized = steps.rules("filters pushed down", "rules: Calcite's and ours, selected by us", PUSHDOWN, optimized);
        /// Constant rows come from a generator on the sink's worker, else on the worker of a source the query reads.
        String generatorHost = !sinkHost.isEmpty() ? sinkHost : sources.stream()
            .flatMap(source -> source.getPhysicalSourcesList().stream()).map(nes.calcite.proto.PhysicalSource::getHost)
            .filter(host -> !host.isEmpty()).findFirst().orElse(null);
        RelNode generated = ValuesFromGenerator.apply(optimized, generatorHost);
        if (generated != optimized) {
            optimized = steps.add("constant rows from a generator", "ours: ValuesFromGenerator", generated);
        }
        rejectUnsupported(optimized);
        optimized = typeCaseResults(optimized);
        optimized = typeUntypedNulls(optimized);
        optimized = NesFunctions.normalize(optimized);
        optimized = steps.add("types made explicit for NebulaStream", "ours: Planner.typeCaseResults, typeUntypedNulls, Unsigned.prepare", Unsigned.prepare(optimized));
        root = root.withRel(optimized);
        if (physical) {
            /// The output names as on the logical path (a name that differs only in case is its column's name).
            root = outputNames(root, relBuilder, steps);
            root = sourceTimeTypes(root, steps);
            if (nestedLoopJoins) {
                root = root.withRel(steps.add("window joins as nested-loop joins", "ours: NesWindowJoin.nestedLoop",
                    NesWindowJoin.nestedLoop(root.rel)));
            }
            List<Placement.Fragment> fragments = toPhysical(volcano, root, topology, sinkHost, steps, sinkFormat, layoutPolicy);
            String explain = describe(fragments);
            return new Result(Plan.builder().executionBehavior(provider.getExecutionBehavior())
                .roots(provider.convertPhysical(fragments)).build(), explain, steps.list());
        }

        root = outputNames(root, relBuilder, steps);
        root = RelRoot.of(steps.add("columns named for NebulaStream", "ours: NesNames", NesNames.apply(root.rel)), root.kind);
        optimized = root.rel;
        Plan.Root substraitRoot;
        try {
            substraitRoot = SubstraitRelVisitor.convert(root, provider);
        } catch (UnsupportedOperationException | IllegalArgumentException e) {
            throw new UnsupportedOperationException(
                "The optimized plan contains something NebulaStream cannot execute (" + e.getMessage() + "):\n"
                    + RelOptUtil.toString(optimized), e);
        }
        return new Result(
            Plan.builder().executionBehavior(provider.getExecutionBehavior()).addRoots(substraitRoot).build(), RelOptUtil.toString(optimized), steps.list());
    }

    /**
     * Upper-cased names of all columns used in DESCRIPTOR(...). Calcite's TUMBLE/HOP only accept TIMESTAMP columns,
     * while NebulaStream event time is an integer in milliseconds, so these columns are declared as TIMESTAMP(3).
     */
    private static Set<String> descriptorColumns(SqlNode query) {
        Set<String> columns = new HashSet<>();
        /// alias -> renamed column (`col AS alias`): a time column renamed in a subquery is still the source's column.
        java.util.Map<String, Set<String>> renamed = new java.util.HashMap<>();
        query.accept(new SqlBasicVisitor<Void>() {
            @Override
            public Void visit(SqlCall call) {
                if (call.getOperator().getName().equalsIgnoreCase("DESCRIPTOR")) { // unvalidated: OTHER_FUNCTION
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand instanceof SqlIdentifier identifier) {
                            columns.add(Util.last(identifier.names).toUpperCase(Locale.ROOT));
                        }
                    }
                }
                if (call.getKind() == SqlKind.AS && call.operand(0) instanceof SqlIdentifier column
                    && call.operand(1) instanceof SqlIdentifier alias) {
                    renamed.computeIfAbsent(Util.last(alias.names).toUpperCase(Locale.ROOT), k -> new HashSet<>())
                        .add(Util.last(column.names).toUpperCase(Locale.ROOT));
                }
                return super.visit(call);
            }
        });
        java.util.ArrayDeque<String> pending = new java.util.ArrayDeque<>(columns);
        while (!pending.isEmpty()) {
            for (String column : renamed.getOrDefault(pending.poll(), Set.of())) {
                if (columns.add(column)) {
                    pending.add(column);
                }
            }
        }
        return columns;
    }

    /// -Dnes.calcite.trace=true prints the plan after every stage.

    /// Our CASE requires every result to have the type of the CASE, but our literals carry no type of their own (it is
    /// derived from their value, e.g. 0 is UINT8); so each result that is a literal or of another type gets a CAST.
    private static RelNode typeCaseResults(RelNode rel) {
        RexShuttle castResults = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                RexCall visited = (RexCall) super.visitCall(call);
                if (visited.getKind() != SqlKind.CASE) {
                    return visited;
                }
                RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
                List<RexNode> operands = new ArrayList<>(visited.getOperands());
                for (int i = 0; i < operands.size(); i++) {
                    boolean isResult = i % 2 == 1 || i == operands.size() - 1; // [when, then, ..., else]
                    RexNode operand = operands.get(i);
                    if (isResult && (operand instanceof RexLiteral
                        || operand.getType().getSqlTypeName() != visited.getType().getSqlTypeName())) {
                        operands.set(i, rexBuilder.makeAbstractCast(
                            rexBuilder.getTypeFactory().createTypeWithNullability(visited.getType(), operand.getType().isNullable()),
                            operand, false));
                    }
                }
                return visited.clone(visited.getType(), operands);
            }
        };
        return rel.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                return super.visit(other).accept(castResults);
            }
        });
    }

    /// A NULL without context, e.g. in NULL < NULL, has Calcite's type NULL, which Substrait lacks. Its value is NULL
    /// whatever its type, so it becomes a NULL of a concrete type.
    private static RelNode typeUntypedNulls(RelNode rel) {
        RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RelDataType integer = rexBuilder.getTypeFactory().createTypeWithNullability(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER), true);
        RexShuttle typeNulls = new RexShuttle() {
            @Override
            public RexNode visitLiteral(RexLiteral literal) {
                return literal.getType().getSqlTypeName() == SqlTypeName.NULL ? rexBuilder.makeNullLiteral(integer) : literal;
            }
        };
        return rel.accept(new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                return super.visit(other).accept(typeNulls);
            }
        });
    }

    /// Joins and aggregations need a window over streams; on finite inputs GlobalWindowRules has converted them already.
    /// Fails on what NebulaStream cannot execute, rather than leaving it to a less helpful error in the conversion.
    private static void rejectUnsupported(RelNode rel) {
        /// We cannot sort, so ORDER BY, LIMIT and OFFSET fail rather than being ignored. (Calcite itself already drops
        /// ORDER BY without LIMIT/OFFSET in subqueries, where it does not change the result.)
        if (rel instanceof Sort) {
            throw new UnsupportedOperationException("ORDER BY, LIMIT and OFFSET are not supported:\n" + RelOptUtil.toString(rel));
        }
        if (rel instanceof Join || rel instanceof Aggregate) {
            throw new UnsupportedOperationException(rel.getRelTypeName() + " without a window (TUMBLE/HOP) is not supported on streams;"
                + " for finite inputs, enable finite_inputs:\n" + RelOptUtil.toString(rel));
        }
        rel.getInputs().forEach(Planner::rejectUnsupported);
    }

    /**
     * The physical plan, in parts that each run on one worker: the sources are expanded per physical source, the logical
     * operators placed on the workers (Placement; without a topology, all run on one worker), and each part turned into
     * the cheapest physical operators and pipelines. A part writes into the query's sink or into a network channel.
     */
    private static List<Placement.Fragment> toPhysical(VolcanoPlanner volcano, RelRoot root, List<nes.calcite.proto.Worker> topology,
                                                       String sinkHost, Steps steps, String sinkFormat,
                                                       nes.calcite.proto.LayoutPolicy layoutPolicy) {
        /// Projections and filters go below the unions of physical sources, so that each source's rows shrink where it runs.
        RelNode logical = steps.rules("filters and projections pushed into each physical source", "rules: Calcite's, selected by us",
            PUSH_INTO_SOURCES, root.project());
        logical = steps.add("watermark assigners after the sources", "ours: Watermarks.assign", Watermarks.assign(logical));
        List<Placement.Part> parts = topology.isEmpty()
            ? List.of(new Placement.Part(null, logical, null, null))
            : Placement.place(logical, topology, sinkHost);
        if (!topology.isEmpty()) {
            steps.add("placed on workers (Placement)", "ours: Placement (ILP with ojAlgo)", parts.stream()
                .map(part -> new PlanGraph.Part(part.host(), part.plan(), new PlanGraph.Output(part.channel(), part.downstreamHost())))
                .toList(), false);
        }

        /// A worker that only forwards a channel needs no planning; all other parts are planned at once.
        java.util.function.Predicate<Placement.Part> forwards =
            part -> part.channel() != null && part.plan() instanceof NesLogical.NetworkSource;
        List<Placement.Part> planned = parts.stream().filter(forwards.negate()).toList();
        NesPhysical.resetFreshNames();
        NesPhysical.RULES.forEach(volcano::addRule);
        Conversions conversions = steps.detailed() ? new Conversions(volcano) : null;
        if (conversions != null) {
            volcano.addListener(conversions);
        }
        RelNode holder = new NesLogical.Parts(logical.getCluster(), logical.getTraitSet(), planned.stream().map(Placement.Part::plan).toList());
        volcano.setRoot(volcano.changeTraits(holder, holder.getTraitSet().replace(NesPhysical.NES)));
        RelNode best;
        try {
            best = Pipelines.strip(volcano.findBestExp());
        } catch (RelOptPlanner.CannotPlanException e) {
            RelNode unsupported = NesPhysical.unsupported(logical);
            throw new UnsupportedOperationException("not supported in physical plans (yet): " + (unsupported == null
                ? "\n" + RelOptUtil.toString(logical)
                : NesPhysical.describeUnsupported(unsupported)));
        }

        List<Placement.Fragment> fragments = new ArrayList<>();
        for (Placement.Part part : parts) {
            RelOptCluster cluster = logical.getCluster();
            RelTraitSet traits = logical.getTraitSet().replace(NesPhysical.NES);
            RelNode sink;
            if (forwards.test(part)) {
                NesLogical.NetworkSource source = (NesLogical.NetworkSource) part.plan();
                /// Forwards the buffers as they are.
                RelNode received = new NesPhysical.NetworkSource(cluster, traits, source.getRowType(), source.channel, source.upstreamHost);
                sink = new NesPhysical.NetworkSink(cluster, traits, List.of(received), part.channel(), part.downstreamHost());
            } else {
                RelNode physical = Pipelines.strip(best.getInput(planned.indexOf(part)));
                /// The rows go into the sink in the sink's format, or into the network in the native format.
                List<Integer> all = java.util.stream.IntStream.range(0, physical.getRowType().getFieldCount()).boxed().toList();
                RelNode emit = new NesPhysical.Emit(cluster, traits, physical, all);
                sink = part.channel() == null
                    ? new NesPhysical.Sink(cluster, traits, List.of(emit), root.fields.rightList())
                    : new NesPhysical.NetworkSink(cluster, traits, List.of(emit), part.channel(), part.downstreamHost());
            }
            fragments.add(new Placement.Fragment(part.host(), sink));
        }
        /// With detailed steps: the conversions one by one, each with its reason.
        List<nes.calcite.proto.RuleApplication> converted = conversions == null ? List.of() : conversions.replay(
            planned.stream().map(Placement.Part::host).toList(),
            planned.stream().map(part -> part.channel() == null ? null : new PlanGraph.Output(part.channel(), part.downstreamHost())).toList());
        steps.add("physical operators (Calcite's choice, with Retain and UnionAll)", "Calcite: VolcanoPlanner, with our operators and converter rules (NesPhysical)", graphParts(fragments), false, converted);
        fragments = fragments.stream().map(f -> new Placement.Fragment(f.host(), Pipelines.finish(f.root()))).toList();
        steps.add("pipelines (Pipelines.finish)", "ours: Pipelines.finish", graphParts(fragments), true);
        Layouts.assign(fragments, sinkFormat, layoutPolicy);
        steps.add("memory layouts (Layouts)", "ours: Layouts.assign", graphParts(fragments), true);
        return fragments;
    }

    /**
     * The placed pipelines per worker, for people (EXPLAIN). Channels, joins, windows and physical sources are numbered in the
     * order they appear, as their ids (random UUIDs, identity hash codes, catalog ids) would make the text of the same query
     * differ from run to run.
     */
    private static String describe(List<Placement.Fragment> fragments) {
        StringBuilder text = new StringBuilder();
        for (Placement.Fragment fragment : fragments) {
            text.append(fragment.host() == null ? "" : "on " + fragment.host() + ":\n").append(RelOptUtil.toString(fragment.root()));
        }
        java.util.Map<String, String> channels = new java.util.LinkedHashMap<>();
        java.util.regex.Matcher uuids = java.util.regex.Pattern
            .compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}").matcher(text);
        StringBuilder numbered = new StringBuilder();
        while (uuids.find()) {
            String channel = channels.computeIfAbsent(uuids.group(), uuid -> "channel-" + (channels.size() + 1));
            uuids.appendReplacement(numbered, channel);
        }
        uuids.appendTail(numbered);
        for (String kind : List.of("join", "window", "physicalSource")) {
            java.util.Map<String, String> ids = new java.util.HashMap<>();
            java.util.regex.Matcher states = java.util.regex.Pattern.compile(kind + "=\\[(\\d+)\\]").matcher(numbered.toString());
            StringBuilder renumbered = new StringBuilder();
            while (states.find()) {
                String id = ids.computeIfAbsent(states.group(1), state -> String.valueOf(ids.size() + 1));
                states.appendReplacement(renumbered, kind + "=[" + id + "]");
            }
            states.appendTail(renumbered);
            numbered = renumbered;
        }
        return numbered.toString();
    }

    private static List<PlanGraph.Part> graphParts(List<Placement.Fragment> fragments) {
        return fragments.stream().map(f -> new PlanGraph.Part(f.host(), f.root(), null)).toList();
    }

    private static RelNode run(HepProgram program, RelNode rel) {
        HepPlanner planner = new HepPlanner(program);
        /// Rules fold constants with their planner's executor, and without one with Calcite's compiling RexUtil.EXECUTOR.
        planner.setExecutor(InterpretingRexExecutor.INSTANCE);
        planner.setRoot(rel);
        return planner.findBestExp();
    }

    /** Calcite's type for one of our field types (non-nullable). name: for the error message. */
    static RelDataType relType(RelDataTypeFactory typeFactory, FieldType fieldType, String name) {
        return switch (fieldType) {
            case INT8 -> typeFactory.createSqlType(SqlTypeName.TINYINT);
            case INT16 -> typeFactory.createSqlType(SqlTypeName.SMALLINT);
            case INT32 -> typeFactory.createSqlType(SqlTypeName.INTEGER);
            case INT64 -> typeFactory.createSqlType(SqlTypeName.BIGINT);
            case UINT8 -> typeFactory.createSqlType(SqlTypeName.UTINYINT);
            case UINT16 -> typeFactory.createSqlType(SqlTypeName.USMALLINT);
            case UINT32 -> typeFactory.createSqlType(SqlTypeName.UINTEGER);
            case UINT64 -> typeFactory.createSqlType(SqlTypeName.UBIGINT);
            case FLOAT32 -> typeFactory.createSqlType(SqlTypeName.REAL);
            case FLOAT64 -> typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case BOOLEAN -> typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case CHAR -> typeFactory.createSqlType(SqlTypeName.CHAR, 1);
            case VARSIZED -> typeFactory.createSqlType(SqlTypeName.VARCHAR);
            case TIMESTAMP_MS -> typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3);
            case FIELD_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException(
                "Field " + name + " has no type");
        };
    }

    static final class SourceTable extends AbstractTable {
        private final Source source;
        private final Set<String> eventTimeFields;

        String name() {
            return source.getName();
        }

        List<nes.calcite.proto.PhysicalSource> physicalSources() {
            return source.getPhysicalSourcesList();
        }

        SourceTable(Source source, Set<String> eventTimeFields) {
            this.source = source;
            this.eventTimeFields = eventTimeFields;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = typeFactory.builder();
            for (Field field : source.getFieldsList()) {
                FieldType fieldType = eventTimeFields.contains(field.getName().toUpperCase(Locale.ROOT))
                    ? FieldType.TIMESTAMP_MS : field.getType();
                RelDataType type = relType(typeFactory, fieldType, source.getName() + "." + field.getName());
                builder.add(field.getName(), typeFactory.createTypeWithNullability(type, field.getNullable()));
            }
            return builder.build();
        }
    }
}
