package nes.calcite.physical;

import nes.calcite.logical.Pinned;
import nes.calcite.logical.AnonymousSources;
import nes.calcite.logical.NesLogical;
import nes.calcite.PlacementFailure;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;
import nes.calcite.proto.Worker;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;

/**
 * Places a logical plan (sources expanded per physical source, see NesLogical) on the workers of a topology, operator by
 * operator, before it becomes physical operators. An integer linear program chooses the workers:
 * <ul>
 *   <li>x[o][w] = 1 if operator o runs on worker w; each operator runs on exactly one worker;</li>
 *   <li>a physical source runs on its worker, the sink on the sink's worker;</li>
 *   <li>a worker runs at most max_operators operators; sources and the sink do not count, as in our own placement;</li>
 *   <li>an operator sends its rows only to its own worker or to a worker downstream of it;</li>
 *   <li>objective: each cut, i.e. an operator whose consumer runs on another worker, costs a fixed amount plus the bytes
 *   it sends (Calcite's estimate), times the hops between the two workers.</li>
 * </ul>
 * The plan is then cut into one part per worker and cut: the rows go over the network along the shortest path, and a
 * worker in between forwards them. Each part becomes physical operators and pipelines on its own, so a pipeline only
 * ends at a cut where the plan needs one. Operators that the engine builds together (a window's build and probe, a
 * source and its parser) are one logical operator here.
 */
public final class Placement {
    private Placement() {}

    /** The fixed cost of a cut (writing and reading the rows again, the network operators), in bytes. */
    private static final double CUT_COST = 1024;

    /**
     * A part of the placed plan, on one worker: the logical plan whose rows go to the sink (channel null) or over the
     * channel to the downstream worker. A part that only forwards a channel is a NesLogical.NetworkSource.
     */
    public record Part(String host, RelNode plan, String channel, String downstreamHost) {}

    /** A part of the physical plan, on one worker: its root is the Sink or a NetworkSink. */
    public record Fragment(String host, RelNode root) {}

    public static List<Part> place(RelNode root, List<Worker> topology, String sinkHost) {
        List<String> hosts = topology.stream().map(Worker::getHost).toList();
        int[][] hops = hops(topology, hosts);

        /// The operators, each with its consumer (null: the root's consumer is the sink).
        List<RelNode> operators = new ArrayList<>();
        Map<RelNode, RelNode> consumer = new IdentityHashMap<>();
        collect(root, null, operators, consumer);

        ExpressionsBasedModel model = new ExpressionsBasedModel();
        Map<RelNode, Variable[]> x = new IdentityHashMap<>();
        for (RelNode operator : operators) {
            x.put(operator, onOneWorker(model, hosts.size()));
            /// A source, and the watermark assigner right after it, run on the source's worker.
            if (operator instanceof Pinned pinned && pinned.host() != null) {
                x.get(operator)[workerOf(hosts, pinned.host(), pinned.label())].level(1);
            }
        }
        Variable[] sink = onOneWorker(model, hosts.size());
        sink[index(hosts, sinkHost)].level(1);

        for (int w = 0; w < hosts.size(); w++) {
            long capacity = topology.get(w).getMaxOperators();
            if (capacity > 0) {
                /// As our own placement counts (operatorCapacityDemand): pinned operators (sources and the assigners after
                /// them) and the sink stay where they are and cost nothing.
                Expression load = model.addExpression().upper(capacity);
                for (RelNode operator : operators) {
                    if (!(operator instanceof Pinned)) {
                        load.set(x.get(operator)[w], 1);
                    }
                }
            }
        }
        RelMetadataQuery metadata = root.getCluster().getMetadataQuery();
        for (RelNode operator : operators) {
            Variable[] to = consumer.get(operator) == null ? sink : x.get(consumer.get(operator));
            double bytes = bytes(metadata, operator);
            for (int a = 0; a < hosts.size(); a++) {
                for (int b = 0; b < hosts.size(); b++) {
                    if (a == b) {
                        continue;
                    }
                    Expression both = model.addExpression().set(x.get(operator)[a], 1).set(to[b], 1);
                    if (hops[a][b] < 0) {
                        both.upper(1);
                    } else {
                        /// cut >= operator on a and consumer on b
                        Variable cut = model.addVariable().lower(0).upper(1).weight(hops[a][b] * (CUT_COST + bytes));
                        both.set(cut, -1).upper(1);
                    }
                }
            }
        }
        Optimisation.Result result = model.minimise();
        if (!result.getState().isFeasible()) {
            throw new PlacementFailure("The plan cannot be placed on the topology (" + result.getState()
                + "): sources and the sink stay on their workers, workers run at most max_operators operators, and data"
                + " only flows downstream.");
        }
        Map<RelNode, Integer> placed = new IdentityHashMap<>();
        operators.forEach(operator -> placed.put(operator, worker(x.get(operator))));

        List<Part> parts = new ArrayList<>();
        RelNode plan = cut(root, placed, topology, hosts, parts);
        int rootWorker = placed.get(root);
        int sinkWorker = index(hosts, sinkHost);
        if (rootWorker != sinkWorker) {
            plan = send(plan, path(topology, hosts, rootWorker, sinkWorker), parts);
        }
        parts.add(0, new Part(sinkHost, plan, null, null));
        return parts;
    }

    /**
     * A copy of the node in which each input that runs on another worker is the receiving end of a cut; the parts that
     * send them are added.
     */
    private static RelNode cut(RelNode node, Map<RelNode, Integer> placed, List<Worker> topology, List<String> hosts, List<Part> parts) {
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            RelNode below = cut(input, placed, topology, hosts, parts);
            int from = placed.get(input);
            int to = placed.get(node);
            inputs.add(from == to ? below : send(below, path(topology, hosts, from, to), parts));
        }
        return inputs.isEmpty() ? node : node.copy(node.getTraitSet(), inputs);
    }

    /** Sends the rows of the plan along the path; returns their receiving end on the last worker. */
    private static RelNode send(RelNode plan, List<String> path, List<Part> parts) {
        RelNode current = plan;
        boolean watermarked = PhysicalRules.watermarked(plan);
        for (int hop = 1; hop < path.size(); hop++) {
            String channel = UUID.randomUUID().toString();
            parts.add(new Part(path.get(hop - 1), current, channel, path.get(hop)));
            current = new NesLogical.NetworkSource(
                plan.getCluster(), plan.getTraitSet(), plan.getRowType(), channel, path.get(hop - 1), watermarked);
        }
        return current;
    }

    private static void collect(RelNode node, RelNode parent, List<RelNode> operators, Map<RelNode, RelNode> consumer) {
        operators.add(node);
        consumer.put(node, parent);
        for (RelNode input : node.getInputs()) {
            collect(input, node, operators, consumer);
        }
    }

    private static Variable[] onOneWorker(ExpressionsBasedModel model, int workers) {
        Variable[] on = new Variable[workers];
        Expression once = model.addExpression().level(1);
        for (int w = 0; w < workers; w++) {
            on[w] = model.addVariable().binary();
            once.set(on[w], 1);
        }
        return on;
    }

    private static int worker(Variable[] on) {
        return IntStream.range(0, on.length).filter(w -> on[w].getValue().signum() > 0).findFirst().orElseThrow();
    }

    /** The bytes an operator produces, as Calcite estimates them (rows times row size). */
    private static double bytes(RelMetadataQuery metadata, RelNode operator) {
        Double rows = metadata.getRowCount(operator);
        Double rowSize = metadata.getAverageRowSize(operator);
        return (rows == null ? 1 : rows) * (rowSize == null ? 8.0 * operator.getRowType().getFieldCount() : rowSize);
    }

    /** The worker of an operator that stays on its host (a source), or a failure that names the operator. */
    private static int workerOf(List<String> hosts, String host, String operator) {
        if (!hosts.contains(host)) {
            throw new PlacementFailure(operator + " was placed on non-existing worker '" + host + "'");
        }
        return hosts.indexOf(host);
    }

    private static int index(List<String> hosts, String host) {
        int index = hosts.indexOf(host);
        if (index < 0) {
            throw new PlacementFailure("worker " + host + " is not in the topology " + hosts);
        }
        return index;
    }

    /** hops[a][b]: the fewest links from worker a downstream to worker b; -1 if b is not downstream of a. */
    private static int[][] hops(List<Worker> topology, List<String> hosts) {
        int[][] hops = new int[hosts.size()][];
        for (int from = 0; from < hosts.size(); from++) {
            hops[from] = distances(topology, hosts, from, null);
        }
        return hops;
    }

    private static int[] distances(List<Worker> topology, List<String> hosts, int from, int[] previous) {
        int[] distance = new int[hosts.size()];
        Arrays.fill(distance, -1);
        distance[from] = 0;
        ArrayDeque<Integer> queue = new ArrayDeque<>(List.of(from));
        while (!queue.isEmpty()) {
            int worker = queue.poll();
            for (String downstream : topology.get(worker).getDownstreamList()) {
                int next = index(hosts, downstream);
                if (distance[next] < 0) {
                    distance[next] = distance[worker] + 1;
                    if (previous != null) {
                        previous[next] = worker;
                    }
                    queue.add(next);
                }
            }
        }
        return distance;
    }

    /** The workers on a shortest path from `from` downstream to `to`, both included. */
    private static List<String> path(List<Worker> topology, List<String> hosts, int from, int to) {
        int[] previous = new int[hosts.size()];
        distances(topology, hosts, from, previous);
        List<String> path = new ArrayList<>();
        for (int worker = to; worker != from; worker = previous[worker]) {
            path.add(0, hosts.get(worker));
        }
        path.add(0, hosts.get(from));
        return path;
    }
}
