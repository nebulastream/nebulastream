import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.protobuf.Any;
import io.substrait.proto.Expression;
import io.substrait.proto.Rel;
import nes.calcite.Planner;
import nes.calcite.proto.Field;
import nes.calcite.proto.FieldType;
import nes.calcite.proto.PhysicalOperator;
import nes.calcite.proto.PhysicalSource;

/**
 * Plans queries over two example sources and writes, per query, the logical plan, every planner step and the physical
 * operators with their pipelines as JSON, for render.py. events has one physical source, events2 two.
 *
 * java -cp target/nes-calcite-planner.jar docs/tools/DumpPlans.java <out.json> <sql>...
 */
public class DumpPlans {
    static nes.calcite.proto.Source source(String name, long... ids) {
        var b = nes.calcite.proto.Source.newBuilder().setName(name);
        for (String f : List.of("id", "val", "ts")) b.addFields(Field.newBuilder().setName(f).setType(FieldType.UINT64));
        for (long id : ids) b.addPhysicalSources(PhysicalSource.newBuilder().setId(id).addAllFieldNames(List.of("ID", "VAL", "TS")));
        return b.build();
    }

    static String q(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }

    public static void main(String[] args) throws Exception {
        var sources = List.of(source("events", 1), source("events2", 2, 3));
        var planner = new Planner();
        List<String> queries = new ArrayList<>();
        for (int i = 1; i < args.length; i++) {
            String sql = args[i];
            String logical = planner.plan(sql, sources, false, false).explain();
            var physical = planner.plan(sql, sources, false, true);
            var proto = new io.substrait.plan.PlanProtoConverter().toProto(physical.plan());
            Map<Integer, String> fn = new HashMap<>();
            for (var e : proto.getExtensionsList())
                if (e.hasExtensionFunction()) fn.put(e.getExtensionFunction().getFunctionAnchor(), e.getExtensionFunction().getName().split(":")[0]);
            List<String> ops = new ArrayList<>();
            collect(proto.getRelations(0).getRoot().getInput(), fn, ops, new int[1]);
            List<String> steps = physical.steps().stream()
                .map(st -> "{\"name\":" + q(st.getName()) + ",\"plan\":" + q(st.getPlan()) + "}").toList();
            queries.add("{\"sql\":" + q(sql) + ",\"logical\":" + q(logical) + ",\"physical\":" + q(physical.explain())
                + ",\"steps\":[" + String.join(",", steps) + "],\"operators\":[" + String.join(",", ops) + "]}");
        }
        Files.writeString(Path.of(args[0]), "[" + String.join(",", queries) + "]");
    }

    /** Collects the operator after its inputs; returns its id. */
    static int collect(Rel rel, Map<Integer, String> fn, List<String> ops, int[] next) throws Exception {
        Any detail;
        List<Rel> inputs;
        if (rel.hasExtensionLeaf()) { detail = rel.getExtensionLeaf().getDetail(); inputs = List.of(); }
        else if (rel.hasExtensionSingle()) { detail = rel.getExtensionSingle().getDetail(); inputs = List.of(rel.getExtensionSingle().getInput()); }
        else { detail = rel.getExtensionMulti().getDetail(); inputs = rel.getExtensionMulti().getInputsList(); }
        List<Integer> inputIds = new ArrayList<>();
        for (Rel input : inputs) inputIds.add(collect(input, fn, ops, next));
        var op = detail.unpack(PhysicalOperator.class);
        int id = next[0]++;
        String kind;
        String text;
        switch (op.getOperatorCase()) {
            case SOURCE_SCAN -> { kind = "SourceScan"; text = "physical source " + op.getSourceScan().getPhysicalSourceId() + ", origin " + op.getSourceScan().getOriginId(); }
            case SCAN -> { kind = "Scan"; text = op.getScan().getParsesSource() ? "parses the source's CSV" : (inputs.size() > 1 ? "merges " + inputs.size() + " inputs" : "reads buffers"); }
            case SELECTION -> { kind = "Selection"; text = expr(op.getSelection().getPredicate(), fn); }
            case MAP -> { kind = "Map"; text = op.getMap().getField() + " := " + expr(op.getMap().getExpression(), fn); }
            case EMIT -> { kind = "Emit"; text = op.getEmit().getForSink() ? "in the sink's format" : "writes buffers"; }
            case SINK -> { kind = "Sink"; text = "the query's sink"; }
            default -> { kind = op.getOperatorCase().toString(); text = ""; }
        }
        String schema = String.join(", ", op.getOutputSchemaList().stream().map(f -> f.getName() + (f.getNullable() ? "?" : "")).toList());
        ops.add("{\"id\":" + id + ",\"kind\":" + q(kind) + ",\"text\":" + q(text) + ",\"schema\":" + q(schema)
            + ",\"pipeline\":" + op.getPipeline() + ",\"inputs\":" + inputIds + "}");
        return id;
    }

    static String expr(Expression e, Map<Integer, String> fn) {
        if (e.hasSelection()) return "$" + e.getSelection().getDirectReference().getStructField().getField();
        if (e.hasLiteral()) {
            var l = e.getLiteral();
            return l.hasI32() ? "" + l.getI32() : l.hasI64() ? "" + l.getI64() : l.hasNull() ? "NULL" : l.hasBoolean() ? "" + l.getBoolean() : "literal";
        }
        if (e.hasCast()) return expr(e.getCast().getInput(), fn);
        if (e.hasIfThen()) {
            var b = new StringBuilder("CASE");
            for (var c : e.getIfThen().getIfsList()) b.append(" WHEN ").append(expr(c.getIf(), fn)).append(" THEN ").append(expr(c.getThen(), fn));
            return b.append(" ELSE ").append(expr(e.getIfThen().getElse(), fn)).append(" END").toString();
        }
        if (e.hasScalarFunction()) {
            var f = e.getScalarFunction();
            String name = fn.get(f.getFunctionReference());
            List<String> args = f.getArgumentsList().stream().map(a -> expr(a.getValue(), fn)).toList();
            Map<String, String> infix = Map.of("add", "+", "subtract", "-", "multiply", "*", "divide", "/", "gt", ">", "gte", ">=",
                "lt", "<", "lte", "<=", "equal", "=", "not_equal", "<>");
            if (infix.containsKey(name) && args.size() == 2) return "(" + args.get(0) + " " + infix.get(name) + " " + args.get(1) + ")";
            if ((name.equals("and") || name.equals("or")) && !args.isEmpty()) return "(" + String.join(" " + name.toUpperCase() + " ", args) + ")";
            return name + "(" + String.join(", ", args) + ")";
        }
        return "?";
    }
}
