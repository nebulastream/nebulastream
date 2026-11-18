package nes.calcite;

import io.substrait.plan.PlanProtoConverter;
import java.nio.file.Files;
import java.nio.file.Path;
import nes.calcite.proto.PlanRequest;
import nes.calcite.proto.PlanResponse;

/**
 * The planner without gRPC: serialized PlanRequest in, serialized PlanResponse out. Errors go into PlanResponse.error.
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
            response.setError(PlannerServer.describe(e)).setUnsupported(!PlannerServer.isInvalidQuery(e));
        }
        return response.build().toByteArray();
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
