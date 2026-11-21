package nes.calcite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import java.util.List;
import nes.calcite.proto.AnonymousSource;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.PhysicalOperator;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.PlanResponse;
import nes.calcite.proto.Source;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowAggregate.Kind;
import nes.calcite.proto.WindowJoin;
import org.junit.jupiter.api.Test;

/** The planner through its native entry point (NativePlanner): a PlanRequest in, a PlanResponse out. */
class PlannerTest {
    /// Every source has a physical source (s1: 11, s2: 21), as plans read physical sources.
    private static final List<Source> SOURCES = List.of(withPhysicalSources(stream("s1"), 11), withPhysicalSources(stream("s2"), 21));

    /** A request the planner rejected: the response's error, and whether the query is unsupported (or invalid). */
    static final class PlanningFailed extends RuntimeException {
        final PlanResponse response;

        PlanningFailed(PlanResponse response) {
            super(response.getError());
            this.response = response;
        }
    }

    /** The planner's response to the request; a source without a physical source gets one. Fails as PlanningFailed. */
    private static PlanResponse respond(PlanRequest request) {
        PlanRequest.Builder builder = request.toBuilder();
        long id = 1000;
        for (int i = 0; i < builder.getSourcesCount(); i++) {
            if (builder.getSources(i).getPhysicalSourcesCount() == 0) {
                builder.setSources(i, withPhysicalSources(builder.getSources(i), id++));
            }
        }
        try {
            PlanResponse response = PlanResponse.parseFrom(NativePlanner.plan(builder.build().toByteArray()));
            if (!response.getError().isEmpty()) {
                throw new PlanningFailed(response);
            }
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    /** A planned query: the response, its Substrait plan and its physical operators (each part's sink first). */
    private record Planned(PlanResponse response, Plan plan, List<PhysicalOperator> operators) {
        String explain() {
            return response.getExplain();
        }

        /// The optimized plan, before the placement and the physical operators.
        String optimized() {
            return response.getStepsList().stream().filter(step -> step.getName().equals("watermark assigners after the sources"))
                .findFirst().orElseThrow().getPlan();
        }

        List<PhysicalOperator> all(java.util.function.Predicate<PhysicalOperator> predicate) {
            return operators.stream().filter(predicate).toList();
        }

        PhysicalOperator one(java.util.function.Predicate<PhysicalOperator> predicate) {
            List<PhysicalOperator> found = all(predicate);
            assertEquals(1, found.size(), explain());
            return found.get(0);
        }

        /// The output names of the query (the sink's).
        List<String> names() {
            return one(PhysicalOperator::hasSink).getOutputSchemaList().stream().map(PhysicalOperator.Field::getName).toList();
        }
    }

    private static Planned planned(PlanRequest request) throws Exception {
        PlanResponse response = respond(request);
        Plan plan = Plan.parseFrom(response.getSubstraitPlan());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        for (var relation : plan.getRelationsList()) {
            collectPhysical(relation.getRoot().getInput(), operators);
        }
        return new Planned(response, plan, operators);
    }

    private static Planned plan(String sql) throws Exception {
        return planned(request(sql, SOURCES));
    }

    private static Planned planFinite(String sql) throws Exception {
        return planned(request(sql, SOURCES).toBuilder().setFiniteInputs(true).build());
    }

    private static PlanRequest request(String sql, List<Source> sources) {
        return PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources).build();
    }

    @Test
    void selectionAndProjection() throws Exception {
        Planned planned = plan("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 10");
        planned.one(PhysicalOperator::hasSelection);
        assertEquals(1, planned.one(PhysicalOperator::hasMap).getMap().getAssignmentsCount());
    }

    @Test
    void outputNamesFollowSqlCasing() throws Exception {
        /// As in standard SQL (and our identifiers): unquoted names are upper case, quoted ones keep their case.
        /// A column passed on keeps its own name if the query spells it in another case (the source's column is "id").
        assertEquals(List.of("id", "V2", "Quoted"), plan("SELECT id, val * 2 AS v2, val AS \"Quoted\" FROM s1").names());
    }

    @Test
    void union() throws Exception {
        /// Each input is a pipeline of its own, into the one sink.
        Planned planned = plan("SELECT id FROM s1 UNION ALL SELECT id FROM s2");
        assertEquals(2, planned.all(PhysicalOperator::hasSourceScan).size());
        planned.one(PhysicalOperator::hasSink);
    }

    @Test
    void unsignedFieldsAreSubstraitUnsignedIntegers() throws Exception {
        Source source = Source.newBuilder().setName("u").addFields(field("x", FieldType.UINT64)).build();
        /// Above 2^63 - 1: compared as a signed value, every x >= 2^63 would be smaller.
        Plan plan = planned(request("SELECT x + 1 FROM u WHERE x > 9223372036854775807", List.of(source))).plan();
        assertTrue(plan.getExtensionUrnsList().stream().anyMatch(u -> u.getUrn().equals("extension:io.substrait:unsigned_integers")),
            plan::toString);
        assertTrue(plan.getExtensionsList().stream().anyMatch(e -> e.hasExtensionType() && e.getExtensionType().getName().equals("u64")),
            plan::toString);
        assertTrue(plan.getExtensionsList().stream().anyMatch(e -> e.getExtensionFunction().getName().equals("gt:u!u64_u!u64")),
            "the filter compares as u64: " + plan);
    }

    @Test
    void mixingSignedAndUnsignedFollowsCalcite() throws Exception {
        Source source = Source.newBuilder().setName("u").addFields(field("x", FieldType.UINT64)).addFields(field("i", FieldType.INT64))
            .addFields(field("s", FieldType.INT32)).build();
        java.util.function.Function<String, String> optimized = sql -> {
            try {
                return planned(request(sql, List.of(source))).optimized();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        };
        /// Arithmetic computes in Calcite's result type (at equal width the unsigned one): the signed operand is cast.
        assertTrue(optimized.apply("SELECT x + i AS a FROM u").contains("+($0, CAST($1):BIGINT UNSIGNED"));
        /// Comparisons compare in the type Calcite casts to: U = I is CAST(U AS BIGINT) = I.
        assertTrue(optimized.apply("SELECT x FROM u WHERE x < i").contains("<(CAST($0):BIGINT"));
        /// ... but against non-negative literals the unsigned value is compared exactly.
        assertTrue(optimized.apply("SELECT x FROM u WHERE x > 5").contains(">($0, CAST(5):BIGINT UNSIGNED"));
        /// Casts between signed and unsigned are allowed (they wrap).
        assertDoesNotThrow(() -> respond(request("SELECT CAST(x AS BIGINT) AS a, CAST(s AS INTEGER UNSIGNED) AS b FROM u", List.of(source))));
    }
    @Test
    void physicalPlan() throws Exception {
        /// s2 has two physical sources: its scan becomes a union of both.
        List<Source> sources = List.of(withPhysicalSources(stream("s1"), 11), withPhysicalSources(stream("s2"), 21, 22));
        PlanRequest request = PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 20 UNION ALL SELECT id, val FROM s2")
            .addAllSources(sources).setSinkFormat("JSON").build();
        PlanResponse response = respond(request);
        Rel root = Plan.parseFrom(response.getSubstraitPlan()).getRelations(0).getRoot().getInput();

        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        collectPhysical(root, operators);
        java.util.Map<PhysicalOperator.OperatorCase, Long> counts = operators.stream()
            .collect(java.util.stream.Collectors.groupingBy(PhysicalOperator::getOperatorCase, java.util.stream.Collectors.counting()));
        /// Each of the 3 sources is a pipeline of its own: parsed by a Scan, it runs through the operators of its branch
        /// of the union, up to an Emit into the sink. No pipeline merges them, and no buffer is read twice.
        assertEquals(java.util.Map.of(
            PhysicalOperator.OperatorCase.SINK, 1L, PhysicalOperator.OperatorCase.EMIT, 3L, PhysicalOperator.OperatorCase.SCAN, 3L,
            PhysicalOperator.OperatorCase.MAP, 1L, PhysicalOperator.OperatorCase.SELECTION, 1L, PhysicalOperator.OperatorCase.SOURCE_SCAN, 3L),
            counts);

        PhysicalOperator sink = operators.get(0);
        assertEquals(List.of("ID", "V2"), sink.getOutputSchemaList().stream().map(PhysicalOperator.Field::getName).toList());
        assertEquals("JSON", operators.get(1).getEmit().getLayout().getFormat().getType(), "the sink's input is emitted in the sink's format");

        List<PhysicalOperator> sourceScans = operators.stream().filter(PhysicalOperator::hasSourceScan).toList();
        assertEquals(java.util.Set.of(11L, 21L, 22L),
            sourceScans.stream().map(o -> o.getSourceScan().getPhysicalSourceId()).collect(java.util.stream.Collectors.toSet()));
        assertEquals(3, sourceScans.stream().map(o -> o.getSourceScan().getOriginId()).distinct().count(), "distinct origins");
        assertEquals(3, operators.stream().filter(o -> o.hasScan() && o.getScan().getLayout().getFormat().getType().equals("CSV")).count(),
            "each scan parses its source's format");

        /// A Map computes one value into one field, appended to the row it gets: val * 2, after S1_ID, S1_VAL, S1_TS.
        PhysicalOperator map = operators.stream().filter(PhysicalOperator::hasMap).findFirst().orElseThrow();
        assertEquals(List.of("S1_ID", "S1_VAL", "S1_TS", map.getMap().getAssignments(0).getField()),
            map.getOutputSchemaList().stream().map(PhysicalOperator.Field::getName).toList());

        assertTrue(operators.stream().filter(PhysicalOperator::hasEmit).allMatch(o -> o.getEmit().getLayout().hasFormat()));
        /// Pipelines: 3 sources, 3 parsing scans and the sink each start one.
        assertEquals(7, operators.stream().map(PhysicalOperator::getPipeline).distinct().count());
    }

    @Test
    void unusedColumnsAreTrimmedAtTheSources() throws Exception {
        /// Only id of each source is used: neither the join state nor its result carries val (physical and anonymous sources).
        List<Source> sources = List.of(withPhysicalSources(stream("s1"), 11), withPhysicalSources(stream("s2"), 21));
        String explain = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT a.id FROM s1 a, s2 b WHERE a.id = b.id")
            .addAllSources(sources).setFiniteInputs(true).build()).getExplain();
        java.util.regex.Matcher stored = java.util.regex.Pattern.compile("JoinBuild\\(.*?stored=\\[\\[(.*?)\\]\\]").matcher(explain);
        int builds = 0;
        while (stored.find()) {
            builds++;
            assertFalse(stored.group(1).contains("VAL"), explain);
        }
        assertEquals(2, builds, explain);

        /// The same with an anonymous source as the right side.
        String anonymous = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT a.id FROM s1 a, TABLE(FILE(schema => 'id UINT64 NOT NULL, val UINT64 NOT NULL', file_path => 'b.csv',"
                + " format => 'CSV', host => 'w1')) b WHERE a.id = b.id")
            .addSourceTypes(nes.calcite.proto.SourceType.newBuilder().setName("File").addOptions("FILE_PATH"))
            .addFormatOptions("TYPE")
            .addSources(onHosts(withPhysicalSources(stream("s1"), 11), "w1"))
            .addTopology(nes.calcite.proto.Worker.newBuilder().setHost("w1")).setSinkHost("w1")
            .setFiniteInputs(true).build()).getExplain();
        stored = java.util.regex.Pattern.compile("JoinBuild\\(.*?stored=\\[\\[(.*?)\\]\\]").matcher(anonymous);
        builds = 0;
        while (stored.find()) {
            builds++;
            assertFalse(stored.group(1).contains("VAL"), anonymous);
        }
        assertEquals(2, builds, anonymous);
    }

    @Test
    void joinKeysInEveryOrBranch() throws Exception {
        /// As TPC-H Q19: the join key and a filter are in each OR branch. They become conditions of their own, so the join has
        /// a key (a hash join, not a nested-loop join over all pairs) and the filter goes below it.
        List<Source> sources = List.of(withPhysicalSources(stream("s1"), 11), withPhysicalSources(stream("s2"), 21));
        String explain = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT SUM(a.val) AS s FROM s1 a, s2 b WHERE (a.id = b.id AND a.val > 1 AND b.val < 5 AND a.ts > 0)"
                + " OR (a.id = b.id AND a.val > 3 AND b.val < 9 AND a.ts > 0)")
            .addAllSources(sources).setFiniteInputs(true).build()).getExplain();
        assertTrue(explain.contains("implementation=[HASH_JOIN]"), explain);
        /// The filters implied for each side: b.val < 5 OR b.val < 9, and a.ts > 0 AND (a.val > 1 OR a.val > 3).
        assertTrue(explain.contains("Selection(condition=[<($1, 9)])"), explain);
        assertTrue(explain.contains("Selection(condition=[AND(>($2, CAST(0):BIGINT UNSIGNED NOT NULL), >($1, 1))])"), explain);
    }

    @Test
    void memoryLayouts() throws Exception {
        /// Two workers: the source's format into the first pipeline, the policy's layout over the network (on both ends of the
        /// channel), rows into the native sink.
        Source s1 = withPhysicalSources(stream("s1"), 11);
        List<Source> sources = List.of(s1.toBuilder().setPhysicalSources(0, s1.getPhysicalSources(0).toBuilder().setHost("w1")).build());
        for (var policy : List.of(nes.calcite.proto.LayoutPolicy.ROWS, nes.calcite.proto.LayoutPolicy.COLUMNS)) {
            PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
                .setSql("SELECT id + 1 AS i FROM s1 WHERE val > 20").addAllSources(sources)
                .addTopology(nes.calcite.proto.Worker.newBuilder().setHost("w1").addDownstream("w2"))
                .addTopology(nes.calcite.proto.Worker.newBuilder().setHost("w2")).setSinkHost("w2")
                .setSinkFormat("NATIVE").setLayoutPolicy(policy).build());
            List<PhysicalOperator> operators = new java.util.ArrayList<>();
            for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
                collectPhysical(relation.getRoot().getInput(), operators);
            }
            var kind = policy == nes.calcite.proto.LayoutPolicy.COLUMNS ? nes.calcite.proto.Layout.FieldGroup.Kind.COLUMN
                : nes.calcite.proto.Layout.FieldGroup.Kind.ROW;
            List<String> scans = operators.stream().filter(PhysicalOperator::hasScan).map(o -> describe(o.getScan().getLayout())).toList();
            List<String> emits = operators.stream().filter(PhysicalOperator::hasEmit).map(o -> describe(o.getEmit().getLayout())).toList();
            /// The source's format; over the network the policy's layout on both ends; rows into the native sink.
            assertEquals(java.util.Set.of("CSV", kind.name()), java.util.Set.copyOf(scans), response.getExplain());
            assertEquals(2, scans.size(), response.getExplain());
            assertEquals(new java.util.HashSet<>(List.of(kind.name(), "ROW")), java.util.Set.copyOf(emits), response.getExplain());
            assertEquals(2, emits.size(), response.getExplain());
        }
    }

    /** The kind of a layout: its format, or the kind of its (only) group. */
    private static String describe(nes.calcite.proto.Layout layout) {
        return layout.hasFormat() ? layout.getFormat().getType() : layout.getNative().getGroups(0).getKind().name();
    }


    @Test
    void modelInference() throws Exception {
        Source flowers = Source.newBuilder().setName("flowers").addFields(field("p1", FieldType.FLOAT32)).addFields(field("p2", FieldType.FLOAT32))
            .addFields(field("ts", FieldType.UINT64)).build();
        nes.calcite.proto.Model iris = nes.calcite.proto.Model.newBuilder().setName("iris")
            .addInputs(field("p1", FieldType.FLOAT32)).addInputs(field("p2", FieldType.FLOAT32))
            .addOutputs(field("setosa", FieldType.FLOAT32)).addOutputs(field("virginica", FieldType.FLOAT32)).build();
        nes.calcite.proto.Model scale = nes.calcite.proto.Model.newBuilder().setName("scale")
            .addInputs(field("setosa", FieldType.FLOAT32)).addOutputs(field("scaled", FieldType.FLOAT32)).build();
        java.util.function.Function<String, PlanRequest> request = sql -> PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql)
            .addSources(flowers).addModels(iris).addModels(scale).build();
        for (String sql : List.of(
            "SELECT * FROM TABLE(MODEL_INFERENCE(TABLE flowers, 'iris'))",
            "SELECT p1, setosa FROM TABLE(MODEL_INFERENCE(TABLE flowers, 'iris')) WHERE setosa > 0.9",
            "SELECT * FROM TABLE(MODEL_INFERENCE((SELECT p1 * 2 AS p1, p2 FROM flowers), 'iris'))",
            "SELECT * FROM TABLE(MODEL_INFERENCE((SELECT * FROM TABLE(MODEL_INFERENCE(TABLE flowers, 'iris'))), 'scale'))")) {
            String plan = planned(request.apply(sql)).optimized();
            assertTrue(plan.contains("model=[iris]"), sql + "\n" + plan);
            assertTrue(!plan.contains("TableFunctionScan"), plan);
        }
        assertTrue(planned(request.apply("SELECT * FROM TABLE(MODEL_INFERENCE((SELECT * FROM TABLE(MODEL_INFERENCE(TABLE flowers, 'iris'))), 'scale'))"))
            .optimized().contains("model=[scale]"));
        PlanningFailed unknown = assertThrows(PlanningFailed.class,
            () -> respond(request.apply("SELECT * FROM TABLE(MODEL_INFERENCE(TABLE flowers, 'nope'))")));
        assertTrue(unknown.getMessage().contains("unknown model"), unknown.getMessage());

        /// The node shows the input columns that the model reads (by name) and the columns it appends.
        String plan = planned(request.apply("SELECT * FROM TABLE(MODEL_INFERENCE((SELECT ts, p2, p1 FROM flowers), 'iris'))")).optimized();
        assertTrue(plan.contains("NesModelInference(model=[iris], reads P1=[$2], reads P2=[$1], appends setosa=[$3 REAL],"
            + " appends virginica=[$4 REAL])"), plan);
        /// An input without a column that the model reads.
        PlanningFailed missing = assertThrows(PlanningFailed.class,
            () -> respond(request.apply("SELECT * FROM TABLE(MODEL_INFERENCE((SELECT p1 FROM flowers), 'iris'))")));
        assertTrue(missing.getMessage().contains("model iris reads the column p2, which its input does not have"), missing.getMessage());
    }

    @Test
    void windowOverSubquery() throws Exception {
        /// Calcite resolves DESCRIPTOR(ts) over a subquery against the subquery's FROM item (WindowInputs works around it):
        /// here ts is column 1 of the subquery, but column 2 of s1.
        String plan = planned(request("SELECT window_start, SUM(val) AS s FROM TABLE(TUMBLE((SELECT val, ts FROM s1 WHERE id > 1),"
            + " DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end", SOURCES)).optimized();
        /// The window's input: the time field (spec.time_field) is ts, i.e. s1's column 2.
        /// time_field is 0 (the default, not printed): TS, which is s1's column 2.
        assertTrue(plan.contains("LogicalProject(TS=[$2], VAL=[$1])") && !plan.contains("time_field"), plan);
        /// A time column renamed in the subquery is still the source's time column.
        assertDoesNotThrow(() -> plan("SELECT window_start, COUNT(*) AS n FROM TABLE(TUMBLE((SELECT val, ts AS t2 FROM s1),"
            + " DESCRIPTOR(t2), INTERVAL '1' SECOND)) GROUP BY window_start, window_end"));
    }

    @Test
    void numericLiterals() throws Exception {
        /// An integer above BIGINT is a BIGINT UNSIGNED; above 2^63 - 1 it cannot cross into the engine (yet).
        PlanningFailed big = assertThrows(PlanningFailed.class,
            () -> respond(request("SELECT id FROM s1 WHERE ts = 18446744073709551615", SOURCES)));
        assertTrue(big.getMessage().contains("unsigned literals above 2^63 - 1"), big.getMessage());
    }

    @Test
    void joinOnKeysOfDifferentTypes() throws Exception {
        /// Calcite casts one join key in a projection above the window function; ProjectWindowTransposeRule moves it below.
        Source left = Source.newBuilder().setName("l").addFields(field("f", FieldType.FLOAT32)).addFields(field("ts", FieldType.UINT64)).build();
        Source right = Source.newBuilder().setName("r").addFields(field("k", FieldType.INT8)).addFields(field("ts", FieldType.UINT64)).build();
        Planned response = planned(request("SELECT a.window_start, a.f, b.k FROM TABLE(TUMBLE(TABLE l, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS a"
            + " JOIN TABLE(TUMBLE(TABLE r, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS b ON a.window_start = b.window_start AND a.f = b.k",
            List.of(left, right)));
        assertTrue(response.optimized().contains("NesWindowJoin") && !response.optimized().contains("TableFunctionScan"), response.optimized());
    }

    @Test
    void timestampFunctions() throws Exception {
        /// SQL's timestamp functions go to the engine as they are (a timestamp is unix milliseconds there).
        String plan = planned(request("SELECT EXTRACT(DAY FROM TIMESTAMP_MILLIS(CAST(ts AS BIGINT))) AS d,"
            + " FORMAT_DATETIME('%Y-%m-%dT%H:%M:%E3SZ', TIMESTAMP_MILLIS(CAST(ts AS BIGINT))) AS f FROM s1", SOURCES)).optimized();
        assertTrue(plan.contains("EXTRACT(FLAG(DAY)") && plan.contains("FORMAT_DATETIME"), plan);
    }

    @Test
    void anonymousSources() throws Exception {
        java.util.function.Function<String, PlanRequest> request = sql -> PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql)
            .addSourceTypes(nes.calcite.proto.SourceType.newBuilder().setName("File").addOptions("FILE_PATH"))
            .addSourceTypes(nes.calcite.proto.SourceType.newBuilder().setName("Generator").addOptions("GENERATOR_SCHEMA").addOptions("SEED"))
            .addFormatOptions("TYPE").addFormatOptions("FIELD_DELIMITER").build();
        Planned response = planned(request.apply("SELECT id, \"name\" FROM TABLE(FILE(schema => 'id UINT64 NOT NULL, \"name\" VARSIZED',"
            + " file_path => 'data.csv', format => 'CSV', format_field_delimiter => ';')) WHERE id > 1"));
        AnonymousSource source = response.one(PhysicalOperator::hasSourceScan).getSourceScan().getAnonymous();
        assertEquals("File", source.getType());
        assertEquals(List.of("ID", "name"), source.getSchemaList().stream().map(Field::getName).toList());
        assertEquals(java.util.Map.of("FILE_PATH", "'data.csv'"), source.getSourceOptionsMap());
        assertEquals(java.util.Map.of("TYPE", "CSV", "FIELD_DELIMITER", "';'"), source.getFormatOptionsMap());
        assertDoesNotThrow(() -> respond(request.apply("SELECT * FROM TABLE(GENERATOR(schema => 'a UINT64 NOT NULL', seed => 1,"
            + " generator_schema => 'SEQUENCE UINT64 0 10 1', format => 'CSV'))")));
        /// Calcite checks the argument names; the schema is required.
        assertThrows(PlanningFailed.class, () -> respond(request.apply("SELECT * FROM TABLE(FILE(schema => 'a UINT64', nope => 1))")));
        assertThrows(PlanningFailed.class, () -> respond(request.apply("SELECT * FROM TABLE(FILE(file_path => 'x'))")));
        /// Also in a (correlated) subquery, which rules only see once it is a join.
        String file = "TABLE(FILE(schema => 'id UINT64 NOT NULL, val UINT64 NOT NULL', file_path => 'data.csv', format => 'CSV'))";
        PlanResponse correlated = respond(request.apply("SELECT r.id FROM " + file + " AS r WHERE r.val > (SELECT AVG(i.val) FROM "
            + file + " AS i WHERE i.id = r.id)").toBuilder().setFiniteInputs(true).build());
        assertFalse(correlated.getExplain().contains("TableFunctionScan"), correlated.getExplain());
    }

    @Test
    void oneMapPerProjection() throws Exception {
        /// A projection computes all its values in one Map, however many there are: here three, one of them a copy.
        PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT id + 1 AS a, val * 2 AS b, id AS c, id AS d FROM s1").addAllSources(List.of(withPhysicalSources(stream("s1"), 11)))
            .build());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        collectPhysical(Plan.parseFrom(response.getSubstraitPlan()).getRelations(0).getRoot().getInput(), operators);
        List<PhysicalOperator> maps = operators.stream().filter(PhysicalOperator::hasMap).toList();
        assertEquals(1, maps.size(), response.getExplain());
        assertEquals(3, maps.get(0).getMap().getAssignmentsCount());
    }

    @Test
    void median() throws Exception {
        /// Calcite has no MEDIAN; ours (NesFunctions) becomes the window's MEDIAN aggregation.
        Planned planned = plan("SELECT window_start, MEDIAN(val) AS m FROM " + T1 + " GROUP BY window_start, window_end");
        assertEquals("MEDIAN", windowBuild(planned).getAggregations(0).getFunction());
    }

    @Test
    void physicalWindowPlan() throws Exception {
        /// s2 has two physical sources: each parses and builds the window in its own pipeline; one probe reads both.
        List<Source> sources = List.of(withPhysicalSources(stream("s2"), 21, 22));
        PlanRequest request = PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT window_start, id, COUNT(*) AS n, SUM(val) AS total FROM " + T2.replace("s2", "s2")
                + " WHERE val > 1 GROUP BY window_start, window_end, id")
            .addAllSources(sources).build();
        PlanResponse response = respond(request);
        System.out.println(response.getExplain());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        collectPhysical(Plan.parseFrom(response.getSubstraitPlan()).getRelations(0).getRoot().getInput(), operators);

        List<PhysicalOperator> builds = operators.stream().filter(PhysicalOperator::hasWindowBuild).toList();
        PhysicalOperator probe = operators.stream().filter(PhysicalOperator::hasWindowProbe).findFirst().orElseThrow();
        assertEquals(2, builds.size(), "one build per physical source");
        assertEquals(2, operators.stream().filter(PhysicalOperator::hasWatermarkAssigner).count());
        assertTrue(builds.stream().allMatch(b -> b.getWindowBuild().getWindow().equals(probe.getWindowProbe().getWindow())),
            "the builds and the probe share one window");
        java.util.Set<Long> sourceOrigins = operators.stream().filter(PhysicalOperator::hasSourceScan)
            .map(o -> o.getSourceScan().getOriginId()).collect(java.util.stream.Collectors.toSet());
        assertEquals(sourceOrigins, java.util.Set.copyOf(probe.getWindowProbe().getInputOriginIdsList()));
        assertTrue(!sourceOrigins.contains(probe.getWindowProbe().getOutputOriginId()));
        /// The engine counts unsigned: COUNT's UINT64 is cast to Calcite's BIGINT after the probe.
        assertEquals(FieldType.UINT64, probe.getOutputSchema(3).getType());
        assertTrue(operators.stream().anyMatch(o -> o.hasMap() && o.getMap().getAssignmentsList().stream().allMatch(a -> a.getExpression().hasCast())));
        /// Pipelines: 2 sources, 2 parse-and-build, the probe, the sink.
        assertEquals(6, operators.stream().map(PhysicalOperator::getPipeline).distinct().count());
    }

    @Test
    void placedPhysicalPlan() throws Exception {
        /// edge -> mid -> cloud; the sink is on cloud. s1 (11) runs on edge, s2 on cloud (21) and mid (22).
        List<Source> sources = List.of(
            onHosts(withPhysicalSources(stream("s1"), 11), "edge"), onHosts(withPhysicalSources(stream("s2"), 21, 22), "cloud", "mid"));
        List<nes.calcite.proto.Worker> topology = List.of(
            worker("edge", 0, "mid"), worker("mid", 0, "cloud"), worker("cloud", 0));
        PlanRequest request = PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 20 UNION ALL SELECT id, val FROM s2")
            .addAllSources(sources).addAllTopology(topology).setSinkHost("cloud").build();
        PlanResponse response = respond(request);
        System.out.println(response.getExplain());
        Plan plan = Plan.parseFrom(response.getSubstraitPlan());

        /// One part per worker and pipeline that sends: the sink on cloud, s1's pipeline on edge, the relay of its buffers
        /// on mid, and s2's pipeline on mid.
        java.util.Map<String, List<List<PhysicalOperator>>> parts = new java.util.TreeMap<>();
        for (var relation : plan.getRelationsList()) {
            List<PhysicalOperator> operators = new java.util.ArrayList<>();
            collectPhysical(relation.getRoot().getInput(), operators);
            String host = operators.get(0).getHost();
            assertTrue(operators.stream().allMatch(o -> o.getHost().equals(host)), "a part runs on one worker");
            parts.computeIfAbsent(host, h -> new java.util.ArrayList<>()).add(operators);
        }
        assertEquals(java.util.Map.of("cloud", 1, "edge", 1, "mid", 2),
            parts.entrySet().stream().collect(java.util.stream.Collectors.toMap(java.util.Map.Entry::getKey, e -> e.getValue().size())));
        assertTrue(parts.get("cloud").get(0).get(0).hasSink());
        assertEquals(2, count(parts.get("cloud"), PhysicalOperator::hasNetworkSource), "from s1's relay and s2 on mid");
        assertEquals(1, count(parts.get("cloud"), PhysicalOperator::hasSourceScan), "s2 on cloud stays local");
        assertEquals(1, count(parts.get("mid"), PhysicalOperator::hasNetworkSource), "the relay of s1");

        /// Every channel has one sender and one receiver, on neighbouring workers.
        List<PhysicalOperator> all = parts.values().stream().flatMap(List::stream).flatMap(List::stream).toList();
        java.util.Map<String, PhysicalOperator> senders = all.stream().filter(PhysicalOperator::hasNetworkSink)
            .collect(java.util.stream.Collectors.toMap(o -> o.getNetworkSink().getChannel(), o -> o));
        java.util.Map<String, PhysicalOperator> receivers = all.stream().filter(PhysicalOperator::hasNetworkSource)
            .collect(java.util.stream.Collectors.toMap(o -> o.getNetworkSource().getChannel(), o -> o));
        assertEquals(senders.keySet(), receivers.keySet());
        assertEquals(3, senders.size());
        senders.forEach((channel, sender) -> {
            PhysicalOperator receiver = receivers.get(channel);
            assertEquals(sender.getNetworkSink().getDownstreamHost(), receiver.getHost());
            assertEquals(receiver.getNetworkSource().getUpstreamHost(), sender.getHost());
        });
        /// Pipeline and origin ids are unique across parts: a worker runs its parts as one plan.
        assertEquals(all.stream().map(PhysicalOperator::getPipeline).distinct().count(),
            all.stream().filter(o -> o.hasSink() || o.hasNetworkSink() || o.hasSourceScan() || o.hasNetworkSource() || o.hasScan()).count());
    }

    @Test
    void placedWindowOverTwoWorkers() throws Exception {
        /// s2's physical sources run on edge and on cloud; the window is one operator for the placement: it runs on one
        /// worker, and the rows of the other source come over the network into its build there.
        List<Source> sources = List.of(onHosts(withPhysicalSources(stream("s2"), 21, 22), "edge", "cloud"));
        PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .setSql("SELECT window_start, COUNT(*) AS n FROM " + T2 + " GROUP BY window_start, window_end")
            .addAllSources(sources).addAllTopology(List.of(worker("edge", 0, "cloud"), worker("cloud", 0)))
            .setSinkHost("cloud").build());
        System.out.println(response.getExplain());
        java.util.Map<String, List<PhysicalOperator>> byHost = new java.util.HashMap<>();
        for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
            List<PhysicalOperator> operators = new java.util.ArrayList<>();
            collectPhysical(relation.getRoot().getInput(), operators);
            byHost.computeIfAbsent(operators.get(0).getHost(), h -> new java.util.ArrayList<>()).addAll(operators);
        }
        String windowHost = byHost.entrySet().stream().filter(e -> e.getValue().stream().anyMatch(PhysicalOperator::hasWindowProbe))
            .map(java.util.Map.Entry::getKey).findFirst().orElseThrow();
        assertEquals(2, byHost.get(windowHost).stream().filter(PhysicalOperator::hasWindowBuild).count(), "both builds with the probe");
        assertEquals(1, byHost.get(windowHost).stream().filter(PhysicalOperator::hasNetworkSource).count(), "the other source's rows");
        /// Rows keep their origins over the network: the window waits for the watermarks of both sources.
        java.util.Set<Long> sourceOrigins = byHost.values().stream().flatMap(List::stream).filter(PhysicalOperator::hasSourceScan)
            .map(o -> o.getSourceScan().getOriginId()).collect(java.util.stream.Collectors.toSet());
        PhysicalOperator probe = byHost.get(windowHost).stream().filter(PhysicalOperator::hasWindowProbe).findFirst().orElseThrow();
        assertEquals(sourceOrigins, java.util.Set.copyOf(probe.getWindowProbe().getInputOriginIdsList()));
    }

    @Test
    void placementRespectsTopology() {
        List<Source> sources = List.of(onHosts(withPhysicalSources(stream("s1"), 11), "edge"));
        String sql = "SELECT id FROM s1 WHERE val > 20";
        /// The sink is not downstream of the source.
        assertThrows(PlanningFailed.class, () -> respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources)
            .addAllTopology(List.of(worker("edge", 0), worker("cloud", 0, "edge"))).setSinkHost("cloud").build()));
        /// As in our own placement, the source and the sink cost no capacity: the filter and the projection are the 2 operators.
        /// With room for one on each worker, the projection runs on cloud: edge sends the filtered rows.
        PlanResponse placed = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources)
            .addAllTopology(List.of(worker("edge", 1, "cloud"), worker("cloud", 1))).setSinkHost("cloud").build());
        String placement = placed.getStepsList().stream().filter(step -> step.getName().startsWith("placed")).findFirst().orElseThrow().getPlan();
        assertTrue(placement.matches("(?s)on cloud, into the sink:\nLogicalProject.*on edge, to cloud:\nLogicalFilter.*"), placement);
    }

    @Test
    void windowRowsFromAnotherWorkerKeepTheirWatermarks() throws Exception {
        /// The join of s1 and s2 runs on edge, the join of its result with s3 on cloud. The joined rows reach cloud with the
        /// watermarks of the first join; an assigner on their event times there would raise the watermark too early.
        List<Source> sources = List.of(onHosts(withPhysicalSources(stream("s1"), 11), "edge"),
            onHosts(withPhysicalSources(stream("s2"), 12), "edge"), onHosts(withPhysicalSources(stream("s3"), 13), "edge"));
        String sql = "SELECT c.id, c.ts, d.val FROM TABLE(HOP((SELECT a.id, a.ts FROM"
            + " TABLE(HOP(TABLE s1, DESCRIPTOR(ts), INTERVAL '0.5' SECOND, INTERVAL '1' SECOND)) AS a"
            + " JOIN TABLE(HOP(TABLE s2, DESCRIPTOR(ts), INTERVAL '0.5' SECOND, INTERVAL '1' SECOND)) AS b"
            + " ON a.window_start = b.window_start AND a.id = b.id), DESCRIPTOR(ts), INTERVAL '0.5' SECOND, INTERVAL '1' SECOND)) AS c"
            + " JOIN TABLE(HOP(TABLE s3, DESCRIPTOR(ts), INTERVAL '0.5' SECOND, INTERVAL '1' SECOND)) AS d"
            + " ON c.window_start = d.window_start AND c.id = d.id";
        PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources)
            .addAllTopology(List.of(worker("edge", 1, "cloud"), worker("cloud", 10))).setSinkHost("cloud").build());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
            collectPhysical(relation.getRoot().getInput(), operators);
        }
        assertEquals(1, operators.stream().filter(o -> o.hasJoinProbe() && o.getHost().equals("edge")).count(), response.getExplain());
        assertTrue(operators.stream().anyMatch(o -> o.hasNetworkSource() && o.getHost().equals("cloud")), response.getExplain());
        /// One assigner per source, none for the joined rows.
        assertEquals(3, operators.stream().filter(PhysicalOperator::hasWatermarkAssigner).count(), response.getExplain());
    }

    @Test
    void eventTimeColumnsKeepTheirSourceTypes() throws Exception {
        /// Calcite reads DESCRIPTOR columns as TIMESTAMP; the joined rows return them with the sources' type, INT64.
        java.util.function.Function<String, Source> signedTime = name -> {
            Source.Builder source = onHosts(withPhysicalSources(stream(name), name.equals("s1") ? 11 : 12), "edge").toBuilder();
            source.setFields(2, source.getFields(2).toBuilder().setType(FieldType.INT64));
            return source.build();
        };
        String sql = "SELECT a.window_start, a.ts, b.ts AS ts2 FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS a"
            + " JOIN TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS b ON a.window_start = b.window_start AND a.id = b.id";
        PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addSources(signedTime.apply("s1"))
            .addSources(signedTime.apply("s2")).addTopology(worker("edge", 10)).setSinkHost("edge").build());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
            collectPhysical(relation.getRoot().getInput(), operators);
        }
        List<FieldType> types = operators.get(0).getOutputSchemaList().stream().map(PhysicalOperator.Field::getType).toList();
        assertEquals(List.of(FieldType.TIMESTAMP_MS, FieldType.INT64, FieldType.INT64), types, response.getExplain());
    }

    @Test
    void theWatermarkAssignerIsTheFirstOperatorAfterTheSource() throws Exception {
        /// The filter runs on cloud (edge has room for none), the assigner still on edge, directly on the source's rows: so
        /// rows that the filter drops advance the watermark, too.
        List<Source> sources = List.of(onHosts(withPhysicalSources(stream("s1"), 11), "edge"));
        String sql = "SELECT window_start, SUM(val) AS s FROM TABLE(TUMBLE((SELECT val, ts FROM s1 WHERE id > 1),"
            + " DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end";
        PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources)
            .addAllTopology(List.of(worker("edge", 1, "cloud"), worker("cloud", 10))).setSinkHost("cloud").build());
        List<PhysicalOperator> operators = new java.util.ArrayList<>();
        for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
            collectPhysical(relation.getRoot().getInput(), operators);
        }
        List<Integer> assigners = java.util.stream.IntStream.range(0, operators.size())
            .filter(i -> operators.get(i).hasWatermarkAssigner()).boxed().toList();
        assertEquals(1, assigners.size(), response.getExplain());
        PhysicalOperator assigner = operators.get(assigners.get(0));
        assertEquals("edge", assigner.getHost(), response.getExplain());
        assertTrue(operators.get(assigners.get(0) + 1).hasScan() && operators.get(assigners.get(0) + 2).hasSourceScan(),
            response.getExplain());
    }

    @Test
    void nestedLoopJoinsOnRequest() throws Exception {
        /// A join with a key is a hash join, unless the request asks for nested-loop joins (join_strategy NESTED_LOOP_JOIN).
        List<Source> sources = List.of(onHosts(withPhysicalSources(stream("s1"), 11), "edge"), onHosts(withPhysicalSources(stream("s2"), 12), "edge"));
        String sql = "SELECT a.id, b.val FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS a"
            + " JOIN TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) AS b ON a.window_start = b.window_start AND a.id = b.id";
        for (boolean nestedLoop : List.of(false, true)) {
            PlanResponse response = respond(PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS).setSql(sql).addAllSources(sources)
                .addTopology(worker("edge", 10)).setSinkHost("edge").setNestedLoopJoins(nestedLoop).build());
            List<PhysicalOperator> operators = new java.util.ArrayList<>();
            for (var relation : Plan.parseFrom(response.getSubstraitPlan()).getRelationsList()) {
                collectPhysical(relation.getRoot().getInput(), operators);
            }
            PhysicalOperator probe = operators.stream().filter(PhysicalOperator::hasJoinProbe).findFirst().orElseThrow();
            assertEquals(nestedLoop ? nes.calcite.proto.WindowJoin.Implementation.NESTED_LOOP_JOIN
                : nes.calcite.proto.WindowJoin.Implementation.HASH_JOIN, probe.getJoinProbe().getJoin().getImplementation(), response.getExplain());
        }
    }

    private static long count(List<List<PhysicalOperator>> parts, java.util.function.Predicate<PhysicalOperator> predicate) {
        return parts.stream().flatMap(List::stream).filter(predicate).count();
    }

    private static nes.calcite.proto.Worker worker(String host, long maxOperators, String... downstream) {
        return nes.calcite.proto.Worker.newBuilder().setHost(host).setMaxOperators(maxOperators).addAllDownstream(List.of(downstream)).build();
    }

    /// Puts the source's physical sources, in order, on the given hosts.
    private static Source onHosts(Source source, String... hosts) {
        Source.Builder builder = source.toBuilder();
        for (int i = 0; i < hosts.length; i++) {
            builder.setPhysicalSources(i, builder.getPhysicalSources(i).toBuilder().setHost(hosts[i]));
        }
        return builder.build();
    }

    private static Source withPhysicalSources(Source source, long... ids) {
        Source.Builder builder = source.toBuilder();
        for (long id : ids) {
            builder.addPhysicalSources(nes.calcite.proto.PhysicalSource.newBuilder().setId(id).setFormat("CSV").addAllFieldNames(
                source.getFieldsList().stream().map(f -> source.getName().toUpperCase() + "_" + f.getName().toUpperCase()).toList()));
        }
        return builder.build();
    }

    private static void collectPhysical(Rel rel, List<PhysicalOperator> operators) throws Exception {
        List<Rel> inputs;
        com.google.protobuf.Any detail;
        if (rel.hasExtensionLeaf()) {
            detail = rel.getExtensionLeaf().getDetail();
            inputs = List.of();
        } else if (rel.hasExtensionSingle()) {
            detail = rel.getExtensionSingle().getDetail();
            inputs = List.of(rel.getExtensionSingle().getInput());
        } else if (rel.hasExtensionMulti()) {
            detail = rel.getExtensionMulti().getDetail();
            inputs = rel.getExtensionMulti().getInputsList();
        } else {
            throw new AssertionError("not a physical operator: " + rel);
        }
        operators.add(detail.unpack(PhysicalOperator.class));
        for (Rel input : inputs) {
            collectPhysical(input, operators);
        }
    }
    private static PhysicalOperator.WindowBuild windowBuild(Planned planned) {
        return planned.one(PhysicalOperator::hasWindowBuild).getWindowBuild();
    }

    private static PhysicalOperator.Join join(Planned planned) {
        return planned.one(PhysicalOperator::hasJoinProbe).getJoinProbe().getJoin();
    }

    /** The pipeline of the only operator that matches. */
    private static int pipelineOf(Planned planned, java.util.function.Predicate<PhysicalOperator> predicate) {
        return planned.one(predicate).getPipeline();
    }

    @Test
    void tumblingWindow() throws Exception {
        PhysicalOperator.WindowBuild build = windowBuild(plan(
            "SELECT window_start, window_end, id, SUM(val) AS s "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                + "GROUP BY window_start, window_end, id"));
        assertEquals(Kind.TUMBLING, build.getWindow().getKind());
        assertEquals(60_000, build.getWindow().getSizeMs());
        assertEquals(60_000, build.getWindow().getSlideMs());
        assertEquals(1, build.getKeyFieldsCount());
        assertEquals("SUM", build.getAggregations(0).getFunction());
    }

    @Test
    void slidingWindowWithCountStar() throws Exception {
        Planned planned = plan(
            "SELECT window_start, window_end, COUNT(*) AS c "
                + "FROM TABLE(HOP(TABLE s1, DESCRIPTOR(ts), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)) "
                + "GROUP BY window_start, window_end");
        PhysicalOperator.WindowBuild build = windowBuild(planned);
        assertEquals(Kind.SLIDING, build.getWindow().getKind());
        assertEquals(60_000, build.getWindow().getSizeMs());
        assertEquals(10_000, build.getWindow().getSlideMs());
        assertEquals(0, build.getKeyFieldsCount());
        assertEquals("COUNT", build.getAggregations(0).getFunction());
        /// COUNT(*) counts the time field, NULLs included.
        assertTrue(build.getAggregations(0).getIncludeNulls());
        assertEquals(build.getTimeField(), build.getAggregations(0).getField());
        /// The window columns keep their names, which differ from the query's only in case (Planner.outputNames).
        assertEquals(List.of("window_start", "window_end", "C"), planned.names());
    }

    @Test
    void windowOverExpressionsAndFilter() throws Exception {
        Planned planned = plan(
            "SELECT window_start, MAX(val * 2) AS m, AVG(val) AS a "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '5' SECOND)) "
                + "WHERE id > 3 "
                + "GROUP BY window_start");
        PhysicalOperator.WindowBuild build = windowBuild(planned);
        assertEquals(5_000, build.getWindow().getSizeMs());
        assertEquals("MAX", build.getAggregations(0).getFunction());
        assertEquals("AVG", build.getAggregations(1).getFunction());
        /// The build's pipeline filters, and a Map computes val * 2 as a plain field.
        int pipeline = pipelineOf(planned, PhysicalOperator::hasWindowBuild);
        assertEquals(pipeline, pipelineOf(planned, PhysicalOperator::hasSelection), planned.explain());
        assertTrue(planned.all(PhysicalOperator::hasMap).stream().anyMatch(map -> map.getPipeline() == pipeline), planned.explain());
    }

    @Test
    void windowOffsetIsRejected() {
        PlanningFailed error = assertThrows(PlanningFailed.class, () -> plan(
            "SELECT window_start, COUNT(*) FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' SECOND)) "
                + "GROUP BY window_start"));
        assertTrue(error.response.getUnsupported(), error.getMessage());
    }

    @Test
    void orderByAndLimitAreRejected() {
        for (String sql : List.of("SELECT id FROM s1 ORDER BY id", "SELECT id FROM s1 LIMIT 3")) {
            PlanningFailed error = assertThrows(PlanningFailed.class, () -> plan(sql));
            assertTrue(error.response.getUnsupported(), sql);
            assertTrue(error.getMessage().contains("ORDER BY, LIMIT and OFFSET"), error.getMessage());
        }
    }

    @Test
    void unknownSourceIsReported() {
        PlanningFailed error = assertThrows(PlanningFailed.class, () -> plan("SELECT * FROM nope"));
        assertFalse(error.response.getUnsupported(), "an invalid query");
        assertTrue(error.getMessage().contains("NOPE"), error.getMessage());
    }

    @Test
    void ddlIsRejected() {
        assertThrows(PlanningFailed.class, () -> plan("INSERT INTO s1 SELECT * FROM s2"));
    }

    @Test
    void filterOnGroupKeyMovesBelowWindow() throws Exception {
        Planned planned = plan(
            "SELECT * FROM (SELECT window_start, id, SUM(val) AS s "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end, id) "
                + "WHERE id > 3 AND s > 10");
        List<Integer> filtering = planned.all(PhysicalOperator::hasSelection).stream().map(PhysicalOperator::getPipeline).toList();
        /// The filter on id runs in the build's pipeline, the one on s after the probe.
        assertTrue(filtering.contains(pipelineOf(planned, PhysicalOperator::hasWindowBuild)), planned.explain());
        assertTrue(filtering.contains(pipelineOf(planned, PhysicalOperator::hasWindowProbe)), planned.explain());
    }

    @Test
    void unusedAggregatesAreDropped() throws Exception {
        PhysicalOperator.WindowBuild build = windowBuild(plan(
            "SELECT window_start, s FROM (SELECT window_start, window_end, SUM(val) AS s, COUNT(*) AS c "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end)"));
        assertEquals(1, build.getAggregationsCount());
        assertEquals("SUM", build.getAggregations(0).getFunction());
    }

    @Test
    void filterOverUnionReachesBothBranches() throws Exception {
        Planned planned = plan("SELECT * FROM (SELECT id FROM s1 UNION ALL SELECT id FROM s2) WHERE id > 1");
        /// Each source's pipeline filters its rows.
        assertEquals(2, planned.all(PhysicalOperator::hasSelection).stream().map(PhysicalOperator::getPipeline).distinct().count(),
            planned.explain());
    }

    private static final String JOIN = "SELECT l.window_start, l.window_end, l.id, l.val, r.val AS rval "
        + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l %s JOIN TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r "
        + "ON l.window_start = r.window_start AND l.window_end = r.window_end AND %s";

    @Test
    void equiJoinUsesHashJoin() throws Exception {
        Planned planned = plan(String.format(JOIN, "", "l.id = r.id"));
        PhysicalOperator.Join join = join(planned);
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join.getImplementation());
        assertEquals(WindowJoin.Type.INNER, join.getType());
        assertEquals(1_000, join.getSizeMs());
        assertEquals(1, join.getLeftKeyFieldsCount());
        assertTrue(planned.one(PhysicalOperator::hasJoinProbe).getJoinProbe().hasCondition());
    }

    @Test
    void thetaJoinUsesNestedLoopJoin() throws Exception {
        Planned planned = plan(String.format(JOIN, "", "l.id < r.id"));
        assertEquals(WindowJoin.Implementation.NESTED_LOOP_JOIN, join(planned).getImplementation());
        PhysicalOperator.JoinProbe probe = planned.one(PhysicalOperator::hasJoinProbe).getJoinProbe();
        assertTrue(probe.hasCondition());
        assertEquals(0, probe.getResidualCount());
    }

    @Test
    void equiJoinWithResidualUsesHashJoin() throws Exception {
        Planned planned = plan(String.format(JOIN, "", "l.id = r.id AND l.val < r.val"));
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join(planned).getImplementation());
        assertEquals(1, planned.one(PhysicalOperator::hasJoinProbe).getJoinProbe().getResidualCount(), "l.val < r.val");
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join(plan(String.format(JOIN, "LEFT", "l.id = r.id AND l.val < r.val"))).getImplementation());
    }

    @Test
    void outerJoin() throws Exception {
        assertEquals(WindowJoin.Type.LEFT, join(plan(String.format(JOIN, "LEFT", "l.id = r.id"))).getType());
    }

    @Test
    void sideFiltersMoveBelowTheJoin() throws Exception {
        Planned planned = plan(String.format(JOIN, "", "l.id = r.id") + " WHERE l.val > 3");
        /// The filter runs in the pipeline of a join build (join reordering may swap the inputs).
        List<Integer> builds = planned.all(PhysicalOperator::hasJoinBuild).stream().map(PhysicalOperator::getPipeline).toList();
        assertTrue(builds.contains(pipelineOf(planned, PhysicalOperator::hasSelection)), planned.explain());
    }

    @Test
    void differentWindowsAreRejected() {
        String sql = String.format(JOIN, "", "l.id = r.id").replaceFirst("INTERVAL '1' SECOND\\)\\) r", "INTERVAL '2' SECOND)) r");
        assertThrows(PlanningFailed.class, () -> plan(sql));
    }

    @Test
    void windowedInputsWithoutWindowEqualityAreRejected() {
        /// Both sides are windowed, but the condition does not join their windows: neither a window join nor (for finite
        /// inputs) a GLOBAL join can read the window columns of a stream.
        String sql = "SELECT l.id FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l"
            + " JOIN TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r ON l.id = r.id";
        for (java.util.concurrent.Callable<Planned> request : List.<java.util.concurrent.Callable<Planned>>of(() -> plan(sql), () -> planFinite(sql))) {
            PlanningFailed error = assertThrows(PlanningFailed.class, request::call);
            assertTrue(error.getMessage().contains("left.window_start = right.window_start"), error.getMessage());
        }
    }

    private static final String T1 = "TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND))";
    private static final String T2 = "TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND))";

    @Test
    void inAndExistsBecomeJoinsWithDeduplicatedSubquery() throws Exception {
        for (String sql : List.of(
            "SELECT l.id FROM " + T1 + " l WHERE l.id IN (SELECT r.id FROM " + T2 + " r WHERE r.window_start = l.window_start)",
            "SELECT l.id FROM " + T1 + " l WHERE EXISTS (SELECT 1 FROM " + T2 + " r WHERE r.window_start = l.window_start AND r.id = l.id)")) {
            Planned planned = plan(sql);
            assertEquals(WindowJoin.Type.INNER, join(planned).getType());
            /// The deduplicating aggregation has no aggregates of its own: it gets a COUNT, which no one reads.
            PhysicalOperator.WindowBuild deduplication = windowBuild(planned);
            assertEquals(1, deduplication.getAggregationsCount(), "deduplicating aggregation");
            assertEquals("COUNT", deduplication.getAggregations(0).getFunction());
        }
    }

    @Test
    void notInAndNotExistsBecomeLeftJoinsKeepingUnmatchedRows() throws Exception {
        for (String sql : List.of(
            "SELECT l.id FROM " + T1 + " l WHERE l.id NOT IN (SELECT r.id FROM " + T2 + " r WHERE r.window_start = l.window_start)",
            "SELECT l.id FROM " + T1 + " l WHERE NOT EXISTS (SELECT 1 FROM " + T2 + " r WHERE r.window_start = l.window_start AND r.id = l.id)")) {
            assertEquals(WindowJoin.Type.LEFT, join(plan(sql)).getType());
        }
    }

    @Test
    void lateralBecomesWindowJoin() throws Exception {
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join(plan("SELECT l.id, x.v FROM " + T1 + " l, LATERAL (SELECT r.val AS v FROM " + T2
            + " r WHERE r.window_start = l.window_start AND r.id = l.id) x")).getImplementation());
    }

    @Test
    void joinOfWindowedAggregations() throws Exception {
        Planned planned = plan("SELECT a.id, a.s, b.s FROM (SELECT window_start, window_end, id, SUM(val) s FROM " + T1
            + " GROUP BY window_start, window_end, id) a JOIN (SELECT window_start, window_end, id, SUM(val) s FROM " + T2
            + " GROUP BY window_start, window_end, id) b ON a.window_start = b.window_start AND a.id = b.id");
        /// Both join builds read the rows of a window probe.
        assertEquals(2, planned.all(PhysicalOperator::hasWindowProbe).size(), planned.explain());
        List<Integer> probes = planned.all(PhysicalOperator::hasWindowProbe).stream().map(PhysicalOperator::getPipeline).toList();
        assertTrue(planned.all(PhysicalOperator::hasJoinBuild).stream().allMatch(build -> probes.contains(build.getPipeline())),
            planned.explain());
    }

    @Test
    void cteIsInlined() throws Exception {
        /// Each use of the CTE reads the source in a pipeline of its own.
        Planned planned = plan("WITH big AS (SELECT id FROM s1 WHERE val > 3) SELECT id FROM big UNION ALL SELECT id FROM big");
        assertEquals(2, planned.all(PhysicalOperator::hasSelection).size(), planned.explain());
    }

    @Test
    void unwindowedJoinsAndAggregationsNeedFiniteInputs() throws Exception {
        String in = "SELECT id FROM s1 WHERE val > 5 AND id IN (SELECT id FROM s2 WHERE val < 85)";
        assertThrows(PlanningFailed.class, () -> plan(in));
        PhysicalOperator.Join join = join(planFinite(in));
        assertEquals(WindowAggregate.Kind.GLOBAL, join.getKind());
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join.getImplementation());

        String groupBy = "SELECT id, COUNT(*) AS c, SUM(val) AS s FROM s1 GROUP BY id";
        assertThrows(PlanningFailed.class, () -> plan(groupBy));
        PhysicalOperator.WindowBuild build = windowBuild(planFinite(groupBy));
        assertEquals(WindowAggregate.Kind.GLOBAL, build.getWindow().getKind());
        assertEquals(2, build.getAggregationsCount());
    }

    @Test
    void globalWindowsReadAConstantTimeColumn() throws Exception {
        Planned planned = planFinite("SELECT id, SUM(val) AS s FROM s1 GROUP BY id");
        /// A Map computes the constant time column, which the build reads as its event time.
        assertTrue(planned.explain().contains("time=[global_window_time]"), planned.explain());
        /// The key, the aggregate (a GLOBAL window has no window columns).
        assertEquals(List.of("id", "S"), planned.names());
    }

    private static Source stream(String name) {
        return Source.newBuilder()
            .setName(name)
            .addFields(field("id", FieldType.INT64))
            .addFields(field("val", FieldType.INT64))
            .addFields(field("ts", FieldType.UINT64))
            .build();
    }

    private static Field field(String name, FieldType type) {
        return Field.newBuilder().setName(name).setType(type).build();
    }
}
