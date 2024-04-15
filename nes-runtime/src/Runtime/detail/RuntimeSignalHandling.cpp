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
#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <folly/experimental/exception_tracer/ExceptionTracerLib.h>
#include <memory>

namespace NES::Runtime {
namespace detail {
static backward::SignalHandling sh;

/// called when a signal is intercepted
void nesErrorHandler(int signal) {
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level) {
        stacktrace = collectStacktrace();
    }
    Exceptions::invokeErrorHandlers(signal, std::move(stacktrace));
}

void nesKillHandler(int signal) {
    ((void) signal);
    NES::Logger::getInstance()->forceFlush();
}

/// called when std::terminate() is invoked
void nesTerminateHandler() {
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level) {
        stacktrace = collectStacktrace();
    }
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
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level) {
        stacktrace = collectStacktrace();
    }
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
#ifdef __linux__
#ifdef NES_ENABLE_CXA_THROW_HOOK
void nesCxaThrowHook(void* ex, std::type_info* info, void (**deleter)(void*)) noexcept {
    using namespace std::string_literals;

    ((void) ex);
    ((void) deleter);
    auto* exceptionName = const_cast<const std::type_info*>(info)->name();
    int status;
    std::unique_ptr<char, void (*)(void*)> realExceptionName(abi::__cxa_demangle(exceptionName, 0, 0, &status), &std::free);
    if (status == 0) {
        constexpr auto* zmqErrorType = "zmq::error_t";
        const auto* exceptionName = const_cast<const char*>(realExceptionName.get());
        if (strcmp(zmqErrorType, exceptionName) != 0) {
            auto stacktrace = NES::collectAndPrintStacktrace();
            NES_ERROR("Exception caught: {} with stacktrace: {}" << exceptionName, stacktrace);
        }
    }
    //Ventura: do not invoke error handlers here, as we let the exception to be intercepted in some catch block
}
#endif
#endif
struct ErrorHandlerLoader {
  public:
    explicit ErrorHandlerLoader() {
        std::set_terminate(nesTerminateHandler);
        std::set_new_handler(nesTerminateHandler);
#ifdef __linux__
#ifdef NES_ENABLE_CXA_THROW_HOOK
        folly::exception_tracer::registerCxaThrowCallback(nesCxaThrowHook);
#endif
#endif
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
        std::signal(SIGKILL, nesKillHandler);
    }
};
static ErrorHandlerLoader loader;

}// namespace detail
}// namespace NES::Runtime