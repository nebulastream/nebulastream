package nes.calcite;

import io.substrait.plan.PlanProtoConverter;
import java.nio.file.Files;
import java.nio.file.Path;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.PlanResponse;

/**
 * The planner's entry point: serialized PlanRequest in, serialized PlanResponse out. Errors go into PlanResponse.error.
 * Entry point of the native library (see NativeEntryPoints) and of a command line for testing the native image:
 * NativePlanner <request.pb>... prints Calcite's plan or the error for each request.
 */
public final class NativePlanner {
    private static final Planner PLANNER = new Planner();

    private NativePlanner() {}

    static byte[] plan(byte[] serializedRequest) {
        PlanResponse.Builder response = PlanResponse.newBuilder();
        try {
            PlanRequest request = PlanRequest.parseFrom(serializedRequest);
            Planner.Result planned = PLANNER.plan(request);
            response.setSubstraitPlan(new PlanProtoConverter().toProto(planned.plan()).toByteString())
                .setExplain(planned.explain())
                .addAllSteps(planned.steps());
        } catch (Throwable e) {
            /// Throwable: e.g. Calcite's AssertionErrors, or an Error of the native image (a missing registration), which
            /// would otherwise only surface as "the native Calcite planner failed".
            response.setError(describe(e)).setUnsupported(!isInvalidQuery(e))
                .setPlacementFailure(PlacementFailure.in(e));
        }
        return response.build().toByteArray();
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
        for (String file : args) {
            PlanResponse response = PlanResponse.parseFrom(plan(Files.readAllBytes(Path.of(file))));
            System.out.println("== " + file);
            System.out.println(response.getError().isEmpty()
                ? response.getExplain() + "substrait: " + response.getSubstraitPlan().size() + " bytes"
                : "error: " + response.getError());
        }
    }
}
