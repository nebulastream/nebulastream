/*
    Stub replacement for cpptrace/forward.hpp.
    Provides minimal forward declarations so cpptrace-dependent headers compile.
*/
#ifndef CPPTRACE_FORWARD_HPP
#define CPPTRACE_FORWARD_HPP

#include <cstdint>

namespace cpptrace {
    using frame_ptr = std::uintptr_t;

    struct raw_trace;
    struct object_trace;
    struct stacktrace;
    struct object_frame;
    struct stacktrace_frame;
    struct safe_object_frame;
}

#endif
