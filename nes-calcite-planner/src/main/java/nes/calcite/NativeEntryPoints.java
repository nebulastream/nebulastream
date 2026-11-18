package nes.calcite;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.UnmanagedMemory;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CCharPointerPointer;
import org.graalvm.nativeimage.c.type.CLongPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

/**
 * C functions of the native library (native-image --shared), next to graal_create_isolate and friends:
 *
 *   int nes_calcite_plan(graal_isolatethread_t*, char* request, long long length, char** response, long long* responseLength);
 *   void nes_calcite_free(graal_isolatethread_t*, char* response);
 *
 * request is a serialized PlanRequest, *response a serialized PlanResponse (see NativePlanner) that the caller releases
 * with nes_calcite_free. Returns 0, or 1 if planning failed with something other than an invalid query (a PlanResponse
 * with an error), e.g. a stack overflow; *response is then not set.
 */
final class NativeEntryPoints {
    private NativeEntryPoints() {}

    @CEntryPoint(name = "nes_calcite_plan")
    static int plan(IsolateThread thread, CCharPointer request, long length, CCharPointerPointer response, CLongPointer responseLength) {
        try {
            byte[] serializedRequest = new byte[(int) length];
            CTypeConversion.asByteBuffer(request, (int) length).get(serializedRequest);
            byte[] serializedResponse = NativePlanner.plan(serializedRequest);
            CCharPointer buffer = UnmanagedMemory.malloc(Math.max(serializedResponse.length, 1));
            CTypeConversion.asByteBuffer(buffer, serializedResponse.length).put(serializedResponse);
            response.write(buffer);
            responseLength.write(serializedResponse.length);
            return 0;
        } catch (Throwable e) {
            return 1;
        }
    }

    @CEntryPoint(name = "nes_calcite_free")
    static void free(IsolateThread thread, CCharPointer response) {
        UnmanagedMemory.free(response);
    }
}
