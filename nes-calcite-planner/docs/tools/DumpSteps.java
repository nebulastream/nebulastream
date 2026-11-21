import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import nes.calcite.Planner;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.PhysicalSource;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.Worker;

/**
 * Plans queries with every planner step in detail (a graph per step, the plan after each rule) and writes them as JSON,
 * for render_steps.py. Topology: worker localhost:8181 -> worker localhost:8282 (workers are named by their gRPC address),
 * the sink on localhost:8282. events has one physical source (on localhost:8181), events2 two (one on each worker).
 *
 * A query prefixed with "finite: " is planned with finite inputs (joins and aggregations without a window get a GLOBAL
 * window).
 *
 * java -cp target/nes-calcite-planner.jar docs/tools/DumpSteps.java <out.json> <sql>...
 *
 * With --requests, it plans the requests that a run wrote to $NES_CALCITE_DUMP_REQUESTS (each `<label>.pb`, e.g. one per
 * systest query; see generate_systest_steps.sh) as they were sent, and records each one's label:
 * java -cp target/nes-calcite-planner.jar docs/tools/DumpSteps.java <out.json> --requests <dir>
 */
public class DumpSteps {
    static nes.calcite.proto.Source source(String name, List<Long> ids, List<String> hosts) {
        var b = nes.calcite.proto.Source.newBuilder().setName(name);
        for (String f : List.of("id", "val", "ts")) b.addFields(Field.newBuilder().setName(f).setType(FieldType.UINT64));
        for (int i = 0; i < ids.size(); i++)
            b.addPhysicalSources(PhysicalSource.newBuilder().setId(ids.get(i)).setHost(hosts.get(i)).addAllFieldNames(List.of("ID", "VAL", "TS")));
        return b.build();
    }

    static String q(String s) {
        StringBuilder out = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\t' -> out.append("\\t");
                default -> out.append(c < 0x20 ? String.format("\\u%04x", (int) c) : String.valueOf(c));
            }
        }
        return out.append('"').toString();
    }

    /** The steps (as JSON) of planning the request, and the error that stopped it; from step `from` on. */
    record Planned(List<String> steps, String error) {}

    static Planned steps(PlanRequest request, Planner planner, int from) {
        List<String> steps = new ArrayList<>();
        String error = null;
        try {
            var all = planner.plan(request).steps();
            for (var step : all.subList(Math.min(from, all.size()), all.size())) {
                List<String> rules = step.getRulesList().stream()
                    .map(r -> "{\"rule\":" + q(r.getRule()) + ",\"ours\":" + r.getOurs() + ",\"plan\":" + q(r.getPlan()) + ",\"dot\":" + q(r.getDot()) + "}").toList();
                steps.add("{\"name\":" + q(step.getName()) + ",\"origin\":" + q(step.getOrigin()) + ",\"plan\":" + q(step.getPlan()) + ",\"dot\":" + q(step.getDot())
                    + ",\"rules\":[" + String.join(",", rules) + "]}");
            }
        } catch (Exception e) {
            error = e.getMessage();
        }
        return new Planned(steps, error);
    }

    static String entry(String label, PlanRequest request, Planner planner) {
        Planned planned = steps(request, planner, 0);
        return "{\"label\":" + q(label) + ",\"sql\":" + q(request.getSql())
            + ",\"error\":" + (planned.error() == null ? "null" : q(planned.error())) + ",\"steps\":[" + String.join(",", planned.steps()) + "]";
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[1].equals("--requests")) {
            var planner = new Planner();
            List<String> queries = new ArrayList<>();
            try (var files = Files.list(Path.of(args[2]))) {
                for (Path file : files.filter(f -> f.toString().endsWith(".pb")).sorted().toList()) {
                    String label = file.getFileName().toString().replaceFirst("\\.pb$", "");
                    PlanRequest request = PlanRequest.parseFrom(Files.readAllBytes(file)).toBuilder().setDetailedSteps(true).build();
                    queries.add(entry(label, request, planner) + "}");
                }
            }
            Files.writeString(Path.of(args[0]), "[" + String.join(",", queries) + "]");
            return;
        }
        var request = PlanRequest.newBuilder().addAllFunctions(Planner.TEST_FUNCTIONS)
            .addSources(source("events", List.of(1L), List.of("localhost:8181")))
            .addSources(source("events2", List.of(2L, 3L), List.of("localhost:8181", "localhost:8282")))
            .addTopology(Worker.newBuilder().setHost("localhost:8181").addDownstream("localhost:8282"))
            .addTopology(Worker.newBuilder().setHost("localhost:8282"))
            .setSinkHost("localhost:8282").setDetailedSteps(true);
        var planner = new Planner();
        List<String> queries = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String sql = args[i];
            boolean finite = sql.startsWith("finite: ");
            sql = finite ? sql.substring("finite: ".length()) : sql;
            queries.add(entry(sql, request.setSql(sql).setFiniteInputs(finite).build(), planner) + "}");
        }
        Files.writeString(Path.of(args[0]), "[" + String.join(",", queries) + "]");
    }
}
