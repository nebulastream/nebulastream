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
#include <Exceptions/RuntimeException.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/Backward/backward.hpp>
#include <Util/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <dlfcn.h>
#include <string>

namespace NES::Runtime {
namespace detail {
static backward::SignalHandling sh;

/// called when a signal is intercepted
void nesErrorHandler(int signal) {
    auto stacktrace = collectAndPrintStacktrace();
    Exceptions::invokeErrorHandlers(signal, std::move(stacktrace));
}

/// called when std::terminate() is invoked
void nesTerminateHandler() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
            // normal termination
            return;
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    Exceptions::invokeErrorHandlers(currentException, std::move(stacktrace));
}

/// called when an exception is not caught in our code
void nesUnexpectedException() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
            throw std::runtime_error("Unknown invalid exception caught");
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    Exceptions::invokeErrorHandlers(currentException, std::move(stacktrace));
}

/// called when a memory allocation fails, e.g., std::bad_alloc
void nesMemoryAllocationHandler() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
            throw std::runtime_error("Memory allocation failed.");
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught: Memory allocation failed");
    }
    Exceptions::invokeErrorHandlers(currentException, std::move(stacktrace));
}

struct ErrorHandlerLoader {
  public:
    explicit ErrorHandlerLoader() {
        std::set_terminate(nesTerminateHandler);
        std::set_new_handler(nesMemoryAllocationHandler);
#ifdef __linux__
        std::set_unexpected(nesUnexpectedException);
#elif defined(__APPLE__)
        // unexpected was removed in C++17 but only apple clang libc did actually remove it..
#else
#error "Unknown platform"
#endif
        std::signal(SIGABRT, nesErrorHandler);
        std::signal(SIGSEGV, nesErrorHandler);
        std::signal(SIGBUS, nesErrorHandler);
    }
};
static ErrorHandlerLoader loader;

}// namespace detail
}// namespace NES::Runtime
//#ifdef __clang__
//#pragma clang diagnostic push
//#pragma clang diagnostic ignored "-Winvalid-noreturn"
//extern "C" {
//_LIBCXXABI_NORETURN void __cxa_throw(void* ex, std::type_info* info, void (*dest)(void*)) {
//    using namespace std::string_literals;
//
//    static void (*const rethrow_func)(void*, void*, void (*)(void*)) =
//        (void (*)(void*, void*, void (*)(void*))) dlsym(RTLD_NEXT, "__cxa_throw");
//
//    auto* exceptionName = reinterpret_cast<const std::type_info*>(info)->name();
//    int status;
//    std::unique_ptr<char, void (*)(void*)> realExceptionName(abi::__cxa_demangle(exceptionName, 0, 0, &status), &std::free);
//    if (status == 0) {
//        auto zmqErrorType = "zmq::error_t"s;
//        if (zmqErrorType.compare(reinterpret_cast<const char*>(realExceptionName.get())) != 0) {
//            auto stacktrace = NES::collectAndPrintStacktrace();
//            NES_ERROR("Exception caught: " << realExceptionName.get() << " with stacktrace: " << stacktrace);
//        }
//    }
//    //Ventura: do not invoke error handlers here, as we let the exception to be intercepted in some catch block
//    rethrow_func(ex, info, dest);
//}
//}
//#pragma clang diagnostic pop
//#else
//#error "Unsupported compiler"
//#endif