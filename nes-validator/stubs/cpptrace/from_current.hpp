/*
    Stub replacement for cpptrace/from_current.hpp.
    Replaces CPPTRACE_TRY/CATCH macros with plain try/catch.
*/
#ifndef CPPTRACE_FROM_CURRENT_HPP
#define CPPTRACE_FROM_CURRENT_HPP

#include <cpptrace/basic.hpp>

#define CPPTRACE_TRY try
#define CPPTRACE_CATCH(...) catch(...)

namespace cpptrace {
    inline const raw_trace& raw_trace_from_current_exception() { static raw_trace t; return t; }
    inline const stacktrace& from_current_exception() { static stacktrace s; return s; }
}

#endif
