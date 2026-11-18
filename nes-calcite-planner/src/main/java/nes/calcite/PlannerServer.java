package nes.calcite;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.substrait.plan.PlanProtoConverter;
import nes.calcite.proto.CalcitePlannerGrpc;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.PlanResponse;

/** gRPC front of {@link Planner}. Usage: java -jar nes-calcite-planner.jar [port] (default 50061). */
public final class PlannerServer extends CalcitePlannerGrpc.CalcitePlannerImplBase {
    private final Planner planner = new Planner();

    @Override
    public void plan(PlanRequest request, StreamObserver<PlanResponse> response) {
        PlanResponse result;
        dump(request);
        try {
            Planner.Result planned = planner.plan(request);
            result = PlanResponse.newBuilder()
                .setSubstraitPlan(new PlanProtoConverter().toProto(planned.plan()).toByteString())
                .setExplain(planned.explain())
                .addAllSteps(planned.steps())
                .build();
        } catch (Exception | AssertionError e) {
            /// AssertionError: Calcite's internal checks, e.g. for odd NULL comparisons; without this the client waits for
            /// its deadline.
            /// UNIMPLEMENTED: a valid query that NebulaStream cannot run (yet); INVALID_ARGUMENT: an invalid one.
            Status status = isInvalidQuery(e) ? Status.INVALID_ARGUMENT : Status.UNIMPLEMENTED;
            response.onError(status.withDescription(describe(e)).asRuntimeException());
            return;
        }
        response.onNext(result);
        response.onCompleted();
    }

    /** -Dnes.calcite.dumpRequests=DIR writes every request to DIR, e.g. to replay them through the native library. */
    private static final String DUMP_DIR = System.getProperty("nes.calcite.dumpRequests");
    private static final java.util.concurrent.atomic.AtomicInteger dumped = new java.util.concurrent.atomic.AtomicInteger();

    private static void dump(PlanRequest request) {
        if (DUMP_DIR == null) {
            return;
        }
        try {
            java.nio.file.Files.write(
                java.nio.file.Path.of(DUMP_DIR, "request-" + ProcessHandle.current().pid() + "-" + dumped.getAndIncrement() + ".pb"), request.toByteArray());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Whether the query itself is wrong (Calcite's parser or validator rejected it), rather than one we cannot run: our
     * own rejections, unconvertible plans, and Calcite's internal errors, e.g. its AssertionErrors.
     */
    static boolean isInvalidQuery(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.apache.calcite.sql.parser.SqlParseException
                || cause instanceof org.apache.calcite.sql.validate.SqlValidatorException
                || cause instanceof org.apache.calcite.runtime.CalciteContextException) {
                return true;
            }
        }
        return false;
    }

    /** Calcite wraps the useful message (with line/column) in a cause chain. */
    static String describe(Throwable e) {
        StringBuilder message = new StringBuilder(String.valueOf(e.getMessage()));
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && !message.toString().contains(cause.getMessage())) {
                message.append(": ").append(cause.getMessage());
            }
        }
        return message.toString();
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 50061;
        Server server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
            .addService(new PlannerServer())
            .build()
            .start();
        System.out.println("nes-calcite-planner listening on port " + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        server.awaitTermination();
    }
}
