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

#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>
#include <memory>
#include <mutex>

namespace NES::Exceptions {

/// this mutex protected the globalErrorListeners vector
static std::recursive_mutex globalErrorListenerMutex;
/// this vector contains system-wide error listeners, e.g., Runtime and Services
static std::vector<std::weak_ptr<ErrorListener>> globalErrorListeners;

void invokeErrorHandlers(std::shared_ptr<std::exception> exception, std::string&& stacktrace) {
    std::unique_lock lock(globalErrorListenerMutex);
    if (globalErrorListeners.empty()) {
        if(stacktrace.empty()){
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << exception->what()
                      << "]\n"
                      << std::endl;
        }
        else{
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << exception->what()
                      << "] with stacktrace=\n"
                      << stacktrace << std::endl;
        }
    }
    for (auto& listener : globalErrorListeners) {
        if (!listener.expired()) {
            listener.lock()->onFatalException(exception, stacktrace);
        }
    }
    Logger::getInstance()->shutdown();
    std::exit(1);
}

void invokeErrorHandlers(int signal, std::string&& stacktrace) {
    std::unique_lock lock(globalErrorListenerMutex);
    if (globalErrorListeners.empty()) {
        if(stacktrace.empty()){
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << strerror(errno)
                      << "] \n"
                      << std::endl;
        }
        else{
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << strerror(errno)
                      << "] with stacktrace=\n"
                      << stacktrace << std::endl;
        }
    }
    for (auto& listener : globalErrorListeners) {
        if (!listener.expired()) {
            listener.lock()->onFatalError(signal, stacktrace);
        }
    }
    Logger::getInstance()->shutdown();
    std::exit(1);
}

void invokeErrorHandlers(const std::string& buffer, std::string&& stacktrace) {
    if(stacktrace.empty()){
        NES_TRACE("invokeErrorHandlers with buffer={}", buffer);
    }
    else{
        NES_TRACE("invokeErrorHandlers with buffer={} trace={}", buffer, stacktrace);
    }
    auto exception = std::make_shared<RuntimeException>(buffer, stacktrace);
    invokeErrorHandlers(exception, std::move(stacktrace));
}

void installGlobalErrorListener(std::shared_ptr<ErrorListener> const& listener) {
    NES_TRACE("installGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    if (listener) {
        globalErrorListeners.emplace_back(listener);
    }
}

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
}// namespace NES::Exceptions
