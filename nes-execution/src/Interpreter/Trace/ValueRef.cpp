
#include <Interpreter/Trace/TraceContext.hpp>
#include <Interpreter/Trace/ValueRef.hpp>

namespace NES::Interpreter {
ValueRef createNextRef() {
    auto ctx = getThreadLocalTraceContext();
    if (ctx) {
        return ctx->createNextRef();
    }
    return ValueRef(0, 0);
}
}// namespace NES::Interpreter
