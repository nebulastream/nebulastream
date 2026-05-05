// WASM-only PRECONDITION/INVARIANT override.
//
// Default macros in nes-common's ErrorHandling.hpp call std::terminate() on
// failure, which lowers to __abort_js in the browser — opaque, no message,
// no stack. This header includes ErrorHandling.hpp first, then redefines the
// macros to throw std::runtime_error so the embind catch in TopologyValidator
// surfaces a readable string. Forced into every TU via -include in the WASM
// CMakeLists.
#pragma once

#include <stdexcept>
#include <ErrorHandling.hpp>
#include <fmt/format.h>

#undef PRECONDITION
#undef INVARIANT

#define PRECONDITION(condition, formatString, ...) \
    do { \
        if (!(condition)) { \
            throw std::runtime_error( \
                fmt::format("Precondition violated: ({}): " formatString, #condition __VA_OPT__(, ) __VA_ARGS__)); \
        } \
    } while (false)

#define INVARIANT(condition, formatString, ...) \
    do { \
        if (!(condition)) { \
            throw std::runtime_error( \
                fmt::format("Invariant violated: ({}): " formatString, #condition __VA_OPT__(, ) __VA_ARGS__)); \
        } \
    } while (false)
