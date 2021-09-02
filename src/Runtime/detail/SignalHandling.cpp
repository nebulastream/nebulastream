/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Exceptions/NesRuntimeException.hpp>
#include <Runtime/ErrorListener.hpp>
#include <Runtime/internal/backward.hpp>
#include <Util/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <memory>
#include <mutex>

namespace NES::Runtime {

/// this mutex protected the globalErrorListeners vector
static std::recursive_mutex globalErrorListenerMutex;
/// this vector contains system-wide error listeners, e.g., Runtime and CoordinatorEngine
static std::vector<std::weak_ptr<ErrorListener>> globalErrorListeners;

/**
 * @brief calls to this function will create a NesRuntimeException that is passed to all system-wide error listeners
 * @param buffer the message of the exception
 * @param stacktrace the stacktrace of where the error was raised
 */
void invokeErrorHandlers(const std::string& buffer, std::string&& stacktrace) {
    NES_TRACE("invokeErrorHandlers with buffer=" << buffer << " trace=" << stacktrace);
    std::unique_lock lock(globalErrorListenerMutex);
    auto exception = std::make_shared<NesRuntimeException>(buffer, stacktrace);
    for (auto& listener : globalErrorListeners) {
        if (!listener.expired()) {
            listener.lock()->onFatalException(exception, stacktrace);
        }
    }
    std::exit(1);
}

/**
 * @brief make an error listener system-wide
 * @param listener the error listener to make system-wide
 */
void installGlobalErrorListener(std::shared_ptr<ErrorListener> const& listener) {
    NES_TRACE("installGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    if (listener) {
        globalErrorListeners.emplace_back(listener);
    }
}

/**
 * @brief remove an error listener system-wide
 * @param listener the error listener to remove system-wide
 */
void removeGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener) {
    NES_TRACE("removeGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    for (auto it = globalErrorListeners.begin(); it != globalErrorListeners.end(); ++it) {
        if (it->lock().get() == listener.get()) {
            globalErrorListeners.erase(it);
            return;
        }
    }
}

namespace detail {
static backward::SignalHandling sh;

/// called when a signal is intercepted
void nesErrorHandler(int signal) {
    auto stacktrace = collectAndPrintStacktrace();
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener.lock()->onFatalError(signal, stacktrace);
        }
    }
    std::exit(1);
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
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener.lock()->onFatalException(currentException, stacktrace);
        }
    }
    std::exit(1);
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
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener.lock()->onFatalException(currentException, stacktrace);
        }
    }
    std::exit(1);
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
