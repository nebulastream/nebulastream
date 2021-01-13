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

#include <NodeEngine/ErrorListener.hpp>
#include <NodeEngine/internal/backtrace.hpp>
#include <Util/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <csignal>
#include <memory>
#include <mutex>

namespace NES::NodeEngine {

static std::mutex globalErrorListenerMutex;
static std::vector<std::shared_ptr<ErrorListener>> globalErrorListeners;

void invokeErrorHandlers(const std::string buffer, std::string&& stacktrace) {
    std::unique_lock lock(globalErrorListenerMutex);
    auto exception = std::make_shared<std::runtime_error>(buffer);
    for (auto& listener : globalErrorListeners) {
        listener->onException(exception, stacktrace);
    }
}

void installGlobalErrorListener(std::shared_ptr<ErrorListener> listener) {
    std::unique_lock lock(globalErrorListenerMutex);
    if (listener) {
        globalErrorListeners.emplace_back(listener);
    }
}

namespace detail {
static backward::SignalHandling sh;

void nesErrorHandler(int signal) {
    auto stacktrace = collectAndPrintStacktrace();
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener->onFatalError(signal, stacktrace);
        }
    }
    std::exit(-1);
}

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
            listener->onException(currentException, stacktrace);
        }
    }
    std::exit(-1);
}

void nesUnexpectedException() {
    auto stacktrace = collectAndPrintStacktrace();
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try {
        if (unknown) {
            std::rethrow_exception(unknown);
        } else {
        }
    } catch (const std::exception& e) {// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    } catch (...) {// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    {
        std::unique_lock lock(globalErrorListenerMutex);
        for (auto& listener : globalErrorListeners) {
            listener->onException(currentException, stacktrace);
        }
    }
    std::exit(-1);
}

struct ErrorHandlerLoader {
  public:
    explicit ErrorHandlerLoader() {
        std::set_terminate(nesTerminateHandler);
        std::set_unexpected(nesUnexpectedException);
        std::signal(SIGABRT, nesErrorHandler);
        std::signal(SIGSEGV, nesErrorHandler);
        std::signal(SIGBUS, nesErrorHandler);
    }
};
static ErrorHandlerLoader loader;

}// namespace detail
}// namespace NES::NodeEngine
