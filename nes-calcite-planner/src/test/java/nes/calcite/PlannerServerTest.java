package nes.calcite;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import java.util.List;
import nes.calcite.proto.CalcitePlannerGrpc;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.PhysicalOperator;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.PlanResponse;
import nes.calcite.proto.Source;
import nes.calcite.proto.WindowAggregate;
import nes.calcite.proto.WindowJoin;
import nes.calcite.proto.WindowAggregate.Function;
import nes.calcite.proto.WindowAggregate.Kind;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PlannerServerTest {
    private static final List<Source> SOURCES = List.of(stream("s1"), stream("s2"));

    private static Server server;
    private static ManagedChannel channel;
    private static CalcitePlannerGrpc.CalcitePlannerBlockingStub stub;

    @BeforeAll
    static void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(new PlannerServer()).build().start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = CalcitePlannerGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void stop() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void selectionAndProjection() throws Exception {
        Rel rel = root(plan("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 10"));
        assertTrue(rel.hasProject());
        assertTrue(rel.getProject().getInput().hasFilter());
        assertTrue(rel.getProject().getInput().getFilter().getInput().hasRead());
    }

    @Test
    void outputNamesKeepTheirSpelling() throws Exception {
        Plan plan = plan("SELECT id, val * 2 AS v2 FROM s1");
        assertEquals(List.of("id", "v2"), plan.getRelations(0).getRoot().getNamesList());
    }

    @Test
    void union() throws Exception {
        assertTrue(root(plan("SELECT id FROM s1 UNION ALL SELECT id FROM s2")).hasSet());
    }

    @Test
    void unsignedFieldsAreSubstraitUnsignedIntegers() throws Exception {
        Source source = Source.newBuilder().setName("u").addFields(field("x", FieldType.UINT64)).build();
        /// Above 2^63 - 1: compared as a signed value, every x >= 2^63 would be smaller.
        Plan plan = Plan.parseFrom(stub.plan(request("SELECT x + 1 FROM u WHERE x > 9223372036854775807", List.of(source)))
            .getSubstraitPlan());
        assertTrue(plan.getExtensionUrnsList().stream().anyMatch(u -> u.getUrn().equals("extension:io.substrait:unsigned_integers")),
            plan::toString);
        assertTrue(plan.getExtensionsList().stream().anyMatch(e -> e.hasExtensionType() && e.getExtensionType().getName().equals("u64")),
            plan::toString);
        assertTrue(plan.toString().contains("CAST") || plan.toString().contains("cast"), "the literal is cast to u64: " + plan);
    }

    @Test
    void mixingSignedAndUnsignedIsRejected() {
        Source source = Source.newBuilder().setName("u").addFields(field("x", FieldType.UINT64)).addFields(field("i", FieldType.INT64))
            .build();
        for (String sql : List.of("SELECT x + i FROM u", "SELECT x FROM u WHERE x < i", "SELECT x FROM u WHERE x < -1",
                "SELECT CAST(x AS BIGINT) FROM u")) {
            StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> stub.plan(request(sql, List.of(source))));
            assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode(), sql);
        }
    }

    @Test
    void physicalPlan() throws Exception {
        /// s2 has two physical sources: its scan becomes a union of both.
        List<Source> sources = List.of(withPhysicalSources(stream("s1"), 11), withPhysicalSources(stream("s2"), 21, 22));
        PlanRequest request = PlanRequest.newBuilder()
            .setSql("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 20 UNION ALL SELECT id, val FROM s2")
            .addAllSources(sources).setPhysical(true).build();
        PlanResponse response = stub.plan(request);
        System.out.println(response.getExplain());
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
        assertEquals(List.of("id", "v2"), sink.getOutputSchemaList().stream().map(PhysicalOperator.Field::getName).toList());
        assertTrue(operators.get(1).getEmit().getForSink(), "the sink's input is emitted in the sink's format");

        List<PhysicalOperator> sourceScans = operators.stream().filter(PhysicalOperator::hasSourceScan).toList();
        assertEquals(java.util.Set.of(11L, 21L, 22L),
            sourceScans.stream().map(o -> o.getSourceScan().getPhysicalSourceId()).collect(java.util.stream.Collectors.toSet()));
        assertEquals(3, sourceScans.stream().map(o -> o.getSourceScan().getOriginId()).distinct().count(), "distinct origins");
        assertEquals(3, operators.stream().filter(o -> o.hasScan() && o.getScan().getParsesSource()).count());

        /// A Map computes one value into one field, appended to the row it gets: val * 2, after S1_ID, S1_VAL, S1_TS.
        PhysicalOperator map = operators.stream().filter(PhysicalOperator::hasMap).findFirst().orElseThrow();
        assertEquals(List.of("S1_ID", "S1_VAL", "S1_TS", map.getMap().getField()),
            map.getOutputSchemaList().stream().map(PhysicalOperator.Field::getName).toList());

        assertTrue(operators.stream().filter(PhysicalOperator::hasEmit).allMatch(o -> o.getEmit().getForSink()));
        /// Pipelines: 3 sources, 3 parsing scans and the sink each start one.
        assertEquals(7, operators.stream().map(PhysicalOperator::getPipeline).distinct().count());
    }

    @Test
    void physicalWindowPlan() throws Exception {
        /// s2 has two physical sources: each parses and builds the window in its own pipeline; one probe reads both.
        List<Source> sources = List.of(withPhysicalSources(stream("s2"), 21, 22));
        PlanRequest request = PlanRequest.newBuilder()
            .setSql("SELECT window_start, id, COUNT(*) AS n, SUM(val) AS total FROM " + T2.replace("s2", "s2")
                + " WHERE val > 1 GROUP BY window_start, window_end, id")
            .addAllSources(sources).setPhysical(true).build();
        PlanResponse response = stub.plan(request);
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
        assertTrue(operators.stream().anyMatch(o -> o.hasMap() && o.getMap().getExpression().hasCast()));
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
        PlanRequest request = PlanRequest.newBuilder()
            .setSql("SELECT id, val * 2 AS v2 FROM s1 WHERE val > 20 UNION ALL SELECT id, val FROM s2")
            .addAllSources(sources).setPhysical(true).addAllTopology(topology).setSinkHost("cloud").build();
        PlanResponse response = stub.plan(request);
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
        PlanResponse response = stub.plan(PlanRequest.newBuilder()
            .setSql("SELECT window_start, COUNT(*) AS n FROM " + T2 + " GROUP BY window_start, window_end")
            .addAllSources(sources).setPhysical(true).addAllTopology(List.of(worker("edge", 0, "cloud"), worker("cloud", 0)))
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
        assertThrows(io.grpc.StatusRuntimeException.class, () -> stub.plan(PlanRequest.newBuilder().setSql(sql).addAllSources(sources)
            .setPhysical(true).addAllTopology(List.of(worker("edge", 0), worker("cloud", 0, "edge"))).setSinkHost("cloud").build()));
        /// The source, the filter, the projection and the sink are 4 operators; the workers have room for 3.
        assertThrows(io.grpc.StatusRuntimeException.class, () -> stub.plan(PlanRequest.newBuilder().setSql(sql).addAllSources(sources)
            .setPhysical(true).addAllTopology(List.of(worker("edge", 2, "cloud"), worker("cloud", 1))).setSinkHost("cloud").build()));
        /// With room for one more on cloud, the projection runs there: edge sends the filtered rows.
        PlanResponse placed = stub.plan(PlanRequest.newBuilder().setSql(sql).addAllSources(sources)
            .setPhysical(true).addAllTopology(List.of(worker("edge", 2, "cloud"), worker("cloud", 2))).setSinkHost("cloud").build());
        String placement = placed.getStepsList().stream().filter(step -> step.getName().startsWith("placed")).findFirst().orElseThrow().getPlan();
        assertTrue(placement.matches("(?s)on cloud, into the sink:\nLogicalProject.*on edge, to cloud:\nLogicalFilter.*"), placement);
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
            builder.addPhysicalSources(nes.calcite.proto.PhysicalSource.newBuilder().setId(id).addAllFieldNames(
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

    @Test
    void tumblingWindow() throws Exception {
        WindowAggregate window = window(plan(
            "SELECT window_start, window_end, id, SUM(val) AS s "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                + "GROUP BY window_start, window_end, id"));
        assertEquals(Kind.TUMBLING, window.getKind());
        assertEquals(60_000, window.getSizeMs());
        assertEquals(60_000, window.getSlideMs());
        assertEquals(1, window.getGroupKeysCount());
        assertEquals(Function.SUM, window.getAggregates(0).getFunction());
    }

    @Test
    void slidingWindowWithCountStar() throws Exception {
        Plan plan = plan(
            "SELECT window_start, window_end, COUNT(*) AS c "
                + "FROM TABLE(HOP(TABLE s1, DESCRIPTOR(ts), INTERVAL '10' SECOND, INTERVAL '1' MINUTE)) "
                + "GROUP BY window_start, window_end");
        WindowAggregate window = window(plan);
        assertEquals(Kind.SLIDING, window.getKind());
        assertEquals(60_000, window.getSizeMs());
        assertEquals(10_000, window.getSlideMs());
        assertEquals(0, window.getGroupKeysCount());
        assertEquals(Function.COUNT, window.getAggregates(0).getFunction());
        assertTrue(!window.getAggregates(0).hasField());
        assertEquals(List.of("window_start", "window_end", "c"), plan.getRelations(0).getRoot().getNamesList());
    }

    @Test
    void windowOverExpressionsAndFilter() throws Exception {
        Plan plan = plan(
            "SELECT window_start, MAX(val * 2) AS m, AVG(val) AS a "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '5' SECOND)) "
                + "WHERE id > 3 "
                + "GROUP BY window_start");
        WindowAggregate window = window(plan);
        assertEquals(5_000, window.getSizeMs());
        assertEquals(Function.MAX, window.getAggregates(0).getFunction());
        assertEquals(Function.AVG, window.getAggregates(1).getFunction());
        /// The window aggregation reads from Project(Filter(Read)): the project computes val * 2 as a plain field.
        Rel input = windowRel(root(plan)).getExtensionSingle().getInput();
        assertTrue(input.hasProject(), input::toString);
        assertTrue(input.getProject().getInput().hasFilter(), input::toString);
    }

    @Test
    void windowOffsetIsRejected() {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> plan(
            "SELECT window_start, COUNT(*) FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' SECOND)) "
                + "GROUP BY window_start"));
        assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode());
    }

    @Test
    void orderByAndLimitAreRejected() {
        for (String sql : List.of("SELECT id FROM s1 ORDER BY id", "SELECT id FROM s1 LIMIT 3")) {
            StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> plan(sql));
            assertEquals(Status.Code.UNIMPLEMENTED, error.getStatus().getCode(), sql);
            assertTrue(error.getStatus().getDescription().contains("ORDER BY, LIMIT and OFFSET"), error.getStatus().getDescription());
        }
    }

    @Test
    void unknownSourceIsReported() {
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class, () -> plan("SELECT * FROM nope"));
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("nope"), error.getStatus().getDescription());
    }

    @Test
    void ddlIsRejected() {
        assertThrows(StatusRuntimeException.class, () -> plan("INSERT INTO s1 SELECT * FROM s2"));
    }

    @Test
    void filterOnGroupKeyMovesBelowWindow() throws Exception {
        Plan plan = plan(
            "SELECT * FROM (SELECT window_start, id, SUM(val) AS s "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end, id) "
                + "WHERE id > 3 AND s > 10");
        Rel window = windowRel(root(plan));
        assertTrue(root(plan).hasFilter() || root(plan).getProject().getInput().hasFilter(), "filter on s stays above");
        assertTrue(containsFilter(window.getExtensionSingle().getInput()), "filter on id moves below the window");
    }

    @Test
    void unusedAggregatesAreDropped() throws Exception {
        WindowAggregate window = window(plan(
            "SELECT window_start, s FROM (SELECT window_start, window_end, SUM(val) AS s, COUNT(*) AS c "
                + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) GROUP BY window_start, window_end)"));
        assertEquals(1, window.getAggregatesCount());
        assertEquals(Function.SUM, window.getAggregates(0).getFunction());
    }

    @Test
    void filterOverUnionReachesBothBranches() throws Exception {
        Rel set = root(plan("SELECT * FROM (SELECT id FROM s1 UNION ALL SELECT id FROM s2) WHERE id > 1"));
        while (!set.hasSet()) {
            set = set.hasProject() ? set.getProject().getInput() : set.getFilter().getInput();
        }
        for (Rel branch : set.getSet().getInputsList()) {
            assertTrue(containsFilter(branch), branch::toString);
        }
    }

    private static boolean containsFilter(Rel rel) {
        if (rel.hasFilter()) {
            return true;
        }
        return rel.hasProject() && containsFilter(rel.getProject().getInput());
    }

    private static final String JOIN = "SELECT l.window_start, l.window_end, l.id, l.val, r.val AS rval "
        + "FROM TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND)) l %s JOIN TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND)) r "
        + "ON l.window_start = r.window_start AND l.window_end = r.window_end AND %s";

    @Test
    void equiJoinUsesHashJoin() throws Exception {
        WindowJoin join = join(plan(String.format(JOIN, "", "l.id = r.id")));
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join.getImplementation());
        assertEquals(WindowJoin.Type.INNER, join.getType());
        assertEquals(1_000, join.getSizeMs());
        assertEquals(2, join.getLeftTimeField());
        assertTrue(join.hasCondition());
    }

    @Test
    void thetaJoinUsesNestedLoopJoin() throws Exception {
        WindowJoin join = join(plan(String.format(JOIN, "", "l.id < r.id")));
        assertEquals(WindowJoin.Implementation.NESTED_LOOP_JOIN, join.getImplementation());
        assertTrue(join.hasCondition());
        assertEquals(0, join.getResidualCount());
    }

    @Test
    void equiJoinWithResidualUsesHashJoin() throws Exception {
        WindowJoin join = join(plan(String.format(JOIN, "", "l.id = r.id AND l.val < r.val")));
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join.getImplementation());
        assertEquals(1, join.getResidualCount(), "l.val < r.val");
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join(plan(String.format(JOIN, "LEFT", "l.id = r.id AND l.val < r.val"))).getImplementation());
    }

    @Test
    void outerJoin() throws Exception {
        assertEquals(WindowJoin.Type.LEFT, join(plan(String.format(JOIN, "LEFT", "l.id = r.id"))).getType());
    }

    @Test
    void sideFiltersMoveBelowTheJoin() throws Exception {
        Rel rel = joinRel(root(plan(String.format(JOIN, "", "l.id = r.id") + " WHERE l.val > 3")));
        /// Join reordering may swap the inputs.
        assertTrue(containsFilter(rel.getExtensionMulti().getInputs(0)) || containsFilter(rel.getExtensionMulti().getInputs(1)), rel::toString);
    }

    @Test
    void differentWindowsAreRejected() {
        String sql = String.format(JOIN, "", "l.id = r.id").replaceFirst("INTERVAL '1' SECOND\\)\\) r", "INTERVAL '2' SECOND)) r");
        assertThrows(StatusRuntimeException.class, () -> plan(sql));
    }

    private static WindowJoin join(Plan plan) throws Exception {
        return joinRel(root(plan)).getExtensionMulti().getDetail().unpack(WindowJoin.class);
    }

    private static Rel joinRel(Rel rel) {
        if (rel.hasExtensionMulti()) {
            return rel;
        }
        if (rel.hasProject()) {
            return joinRel(rel.getProject().getInput());
        }
        if (rel.hasFilter()) {
            return joinRel(rel.getFilter().getInput());
        }
        throw new AssertionError("no window join in " + rel);
    }

    private static final String T1 = "TABLE(TUMBLE(TABLE s1, DESCRIPTOR(ts), INTERVAL '1' SECOND))";
    private static final String T2 = "TABLE(TUMBLE(TABLE s2, DESCRIPTOR(ts), INTERVAL '1' SECOND))";

    @Test
    void inAndExistsBecomeJoinsWithDeduplicatedSubquery() throws Exception {
        for (String sql : List.of(
            "SELECT l.id FROM " + T1 + " l WHERE l.id IN (SELECT r.id FROM " + T2 + " r WHERE r.window_start = l.window_start)",
            "SELECT l.id FROM " + T1 + " l WHERE EXISTS (SELECT 1 FROM " + T2 + " r WHERE r.window_start = l.window_start AND r.id = l.id)")) {
            Rel join = joinRel(root(plan(sql)));
            WindowJoin spec = join.getExtensionMulti().getDetail().unpack(WindowJoin.class);
            assertEquals(WindowJoin.Type.INNER, spec.getType());
            assertEquals(0, window(join.getExtensionMulti().getInputs(1)).getAggregatesCount(), "deduplicating aggregation");
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
        Rel join = joinRel(root(plan("SELECT a.id, a.s, b.s FROM (SELECT window_start, window_end, id, SUM(val) s FROM " + T1
            + " GROUP BY window_start, window_end, id) a JOIN (SELECT window_start, window_end, id, SUM(val) s FROM " + T2
            + " GROUP BY window_start, window_end, id) b ON a.window_start = b.window_start AND a.id = b.id")));
        assertTrue(join.getExtensionMulti().getInputs(0).hasExtensionSingle());
        assertTrue(join.getExtensionMulti().getInputs(1).hasExtensionSingle());
    }

    @Test
    void cteIsInlined() throws Exception {
        assertTrue(root(plan("WITH big AS (SELECT id FROM s1 WHERE val > 3) SELECT id FROM big UNION ALL SELECT id FROM big")).hasSet());
    }

    private static WindowAggregate window(Rel rel) throws Exception {
        return windowRel(rel).getExtensionSingle().getDetail().unpack(WindowAggregate.class);
    }

    @Test
    void unwindowedJoinsAndAggregationsNeedFiniteInputs() throws Exception {
        String in = "SELECT id FROM s1 WHERE val > 5 AND id IN (SELECT id FROM s2 WHERE val < 85)";
        assertThrows(StatusRuntimeException.class, () -> plan(in));
        WindowJoin join = join(planFinite(in));
        assertEquals(WindowAggregate.Kind.GLOBAL, join.getKind());
        assertEquals(WindowJoin.Implementation.HASH_JOIN, join.getImplementation());

        String groupBy = "SELECT id, COUNT(*) AS c, SUM(val) AS s FROM s1 GROUP BY id";
        assertThrows(StatusRuntimeException.class, () -> plan(groupBy));
        WindowAggregate aggregate = window(planFinite(groupBy));
        assertEquals(WindowAggregate.Kind.GLOBAL, aggregate.getKind());
        assertEquals(2, aggregate.getAggregatesCount());
    }

    private static Plan planFinite(String sql) throws Exception {
        PlanRequest request = request(sql, SOURCES).toBuilder().setFiniteInputs(true).build();
        return Plan.parseFrom(stub.plan(request).getSubstraitPlan());
    }

    private static Plan plan(String sql) throws Exception {
        PlanResponse response = stub.plan(request(sql, SOURCES));
        System.out.println(sql + "\n" + response.getExplain());
        return Plan.parseFrom(response.getSubstraitPlan());
    }

    private static PlanRequest request(String sql, List<Source> sources) {
        return PlanRequest.newBuilder().setSql(sql).addAllSources(sources).build();
    }

    private static Rel root(Plan plan) {
        return plan.getRelations(0).getRoot().getInput();
    }

    private static WindowAggregate window(Plan plan) throws Exception {
        return windowRel(root(plan)).getExtensionSingle().getDetail().unpack(WindowAggregate.class);
    }

    private static Rel windowRel(Rel rel) {
        if (rel.hasExtensionSingle()) {
            return rel;
        }
        if (rel.hasProject()) {
            return windowRel(rel.getProject().getInput());
        }
        if (rel.hasFilter()) {
            return windowRel(rel.getFilter().getInput());
        }
        throw new AssertionError("no window aggregation in " + rel);
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
