/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Util/Signal.hpp>

#include <csignal>
#include <tuple>
#include <unistd.h>
#include <cpptrace/basic.hpp>
#include <cpptrace/cpptrace.hpp>
#include <cpptrace/utils.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>

namespace
{
void printStackTrace()
{
    auto trace = cpptrace::raw_trace::current(3);
    trace.resolve().print_with_snippets();
}

const char* signalName(int signal)
{
    switch (signal)
    {
        case SIGSEGV:
            return "SIGSEGV";
        case SIGFPE:
            return "SIGFPE";
        default:
            return "unknown signal";
    }
}

/// Technically, it is unsafe to allocate in a signal handler. Collecting the stacktrace and resolving the stacktrace are thus also unsafe.
/// However, using libunwind (which is required for signal safe backtraces) does not function with the jit compiled code.
/// And resolving symbols would require fork/exec a new process with a dedicated binary which needs to be shipped alongside NebulaStream.
/// For now the signal handler is unsafe and will potentially not work with a corrupted heap, which is still an improvement over not having
/// any idea where the system has crashed.
[[maybe_unused]] void SignalHandler(int signal)
{
    fmt::print(stderr, "Caught signal {}\n", signalName(signal));
    printStackTrace();
    _exit(signal);
}

[[maybe_unused]] void registerSignalHandler(int signalNo, void (*handler)(int))
{
    std::ignore = signal(signalNo, handler);
}
}

#if defined(__has_feature)
    #if __has_feature(address_sanitizer)
        #define NES_BUILT_WITH_ASAN 1
    #endif
#endif

void NES::setupSignalHandlers()
{
    cpptrace::register_terminate_handler();
#ifdef NES_BUILT_WITH_ASAN
    /// When built with AddressSanitizer, do NOT install our own SIGSEGV/SIGFPE handlers.
    /// ASan installs its own handler that produces a precise report (faulting address, read/write,
    /// and — for poisoned-heap accesses — allocation/free stacks). Overriding it with our cpptrace
    /// handler would suppress that report. SIGABRT (used by ASan to terminate) is left untouched.
#else
    registerSignalHandler(SIGSEGV, SignalHandler);
    registerSignalHandler(SIGFPE, SignalHandler);
#endif
}
