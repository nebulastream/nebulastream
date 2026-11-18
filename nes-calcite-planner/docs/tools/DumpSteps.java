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
 * A query prefixed with "logical: " is planned as a logical plan with finite inputs (for what physical plans cannot run
 * yet, e.g. joins).
 *
 * java -cp target/nes-calcite-planner.jar docs/tools/DumpSteps.java <out.json> <sql>...
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

    public static void main(String[] args) throws Exception {
        var request = PlanRequest.newBuilder()
            .addSources(source("events", List.of(1L), List.of("localhost:8181")))
            .addSources(source("events2", List.of(2L, 3L), List.of("localhost:8181", "localhost:8282")))
            .addTopology(Worker.newBuilder().setHost("localhost:8181").addDownstream("localhost:8282"))
            .addTopology(Worker.newBuilder().setHost("localhost:8282"))
            .setSinkHost("localhost:8282").setPhysical(true).setDetailedSteps(true);
        var planner = new Planner();
        List<String> queries = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String sql = args[i];
            boolean logical = sql.startsWith("logical: ");
            sql = logical ? sql.substring("logical: ".length()) : sql;
            List<String> steps = new ArrayList<>();
            String error = null;
            try {
                for (var step : planner.plan(request.setSql(sql).setPhysical(!logical).setFiniteInputs(logical).build()).steps()) {
                    List<String> rules = step.getRulesList().stream()
                        .map(r -> "{\"rule\":" + q(r.getRule()) + ",\"ours\":" + r.getOurs() + ",\"plan\":" + q(r.getPlan()) + ",\"dot\":" + q(r.getDot()) + "}").toList();
                    steps.add("{\"name\":" + q(step.getName()) + ",\"origin\":" + q(step.getOrigin()) + ",\"plan\":" + q(step.getPlan()) + ",\"dot\":" + q(step.getDot())
                        + ",\"rules\":[" + String.join(",", rules) + "]}");
                }
            } catch (Exception e) {
                error = e.getMessage();
            }
            queries.add("{\"sql\":" + q(sql) + ",\"logical\":" + logical + ",\"error\":" + (error == null ? "null" : q(error)) + ",\"steps\":["
                + String.join(",", steps) + "]}");
        }
        Files.writeString(Path.of(args[0]), "[" + String.join(",", queries) + "]");
    }
}
