#include <Nautilus/Tracing/TraceUtil.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>

namespace NES::Nautilus::Tracing::TraceUtil {

[[maybe_unused, nodiscard]] bool inTracer() {
    if(auto traceContext = TraceContext::get()){
        return true;
    }
    return false;
}

[[maybe_unused, nodiscard]] bool inInterpreter() {
    return !inTracer();
}



}