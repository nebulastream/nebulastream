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
#include <csignal>
#include <memory>
#include <Exceptions/SignalHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StacktraceLoader.hpp>

namespace NES::Runtime
{
namespace detail
{

/// called when a signal is intercepted
void nesErrorHandler(int signal)
{
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level)
    {
        stacktrace = collectStacktrace();
    }
    Exceptions::invokeErrorHandlers(signal, std::move(stacktrace));
}

void nesKillHandler(int signal)
{
    ((void)signal);
    NES::Logger::getInstance()->forceFlush();
}

/// called when std::terminate() is invoked
void nesTerminateHandler()
{
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level)
    {
        stacktrace = collectStacktrace();
    }
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try
    {
        if (unknown)
        {
            std::rethrow_exception(unknown);
        }
        else
        {
            /// normal termination
            return;
        }
    }
    catch (const std::exception& e)
    { /// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    }
    catch (...)
    { /// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    Exceptions::invokeErrorHandlers(currentException, std::move(stacktrace));
}

/// called when an exception is not caught in our code
void nesUnexpectedException()
{
    auto currentlevel = NES::getLogLevel(NES::Logger::getInstance()->getCurrentLogLevel());
    auto level = NES::getLogLevel(NES::LogLevel::LOG_DEBUG);
    std::string stacktrace;
    if (currentlevel >= level && NES_COMPILE_TIME_LOG_LEVEL >= level)
    {
        stacktrace = collectStacktrace();
    }
    auto unknown = std::current_exception();
    std::shared_ptr<std::exception> currentException;
    try
    {
        if (unknown)
        {
            std::rethrow_exception(unknown);
        }
        else
        {
            throw std::runtime_error("Unknown invalid exception caught");
        }
    }
    catch (const std::exception& e)
    { /// for proper `std::` exceptions
        currentException = std::make_shared<std::exception>(e);
    }
    catch (...)
    { /// last resort for things like `throw 1;`
        currentException = std::make_shared<std::runtime_error>("Unknown exception caught");
    }
    Exceptions::invokeErrorHandlers(currentException, std::move(stacktrace));
}

struct ErrorHandlerLoader
{
public:
    explicit ErrorHandlerLoader()
    {
        std::set_terminate(nesTerminateHandler);
        std::set_new_handler(nesTerminateHandler);
        std::signal(SIGABRT, nesErrorHandler);
        std::signal(SIGSEGV, nesErrorHandler);
        std::signal(SIGBUS, nesErrorHandler);
        std::signal(SIGKILL, nesKillHandler);
    }
};
static ErrorHandlerLoader loader;

}
}
