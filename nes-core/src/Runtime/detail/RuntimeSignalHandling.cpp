#include <Exceptions/SignalHandling.hpp>
#include <Exceptions/RuntimeException.hpp>
#include <Util/Backward/backward.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <memory>

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

struct ErrorHandlerLoader {
  public:
    explicit ErrorHandlerLoader() {
        std::set_terminate(nesTerminateHandler);
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