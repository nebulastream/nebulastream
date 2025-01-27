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

#include <memory>
#include <mutex>
#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>

namespace NES::Exceptions
{

/// this mutex protected the globalErrorListeners vector
static std::recursive_mutex globalErrorListenerMutex;
/// this vector contains system-wide error listeners, e.g., Runtime and Services
static std::vector<std::weak_ptr<ErrorListener>> globalErrorListeners;

void invokeErrorHandlers(int signal, std::string&& stacktrace)
{
    std::unique_lock lock(globalErrorListenerMutex);
    if (globalErrorListeners.empty())
    {
        if (stacktrace.empty())
        {
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << strerror(errno) << "] \n"
                      << std::endl;
        }
        else
        {
            std::cerr << "No error listener is set, you need to revise your bin logic\n got error=[" << strerror(errno)
                      << "] with stacktrace=\n"
                      << stacktrace << std::endl;
        }
    }
    for (auto& listener : globalErrorListeners)
    {
        if (!listener.expired())
        {
            listener.lock()->onFatalError(signal, stacktrace);
        }
    }
    Logger::getInstance()->shutdown();
    std::exit(1);
}

void installGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener)
{
    NES_TRACE("installGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    if (listener)
    {
        globalErrorListeners.emplace_back(listener);
    }
}

void removeGlobalErrorListener(const std::shared_ptr<ErrorListener>& listener)
{
    NES_TRACE("removeGlobalErrorListener");
    std::unique_lock lock(globalErrorListenerMutex);
    for (auto it = globalErrorListeners.begin(); it != globalErrorListeners.end(); ++it)
    {
        if (it->lock().get() == listener.get())
        {
            globalErrorListeners.erase(it);
            return;
        }
    }
}
}
