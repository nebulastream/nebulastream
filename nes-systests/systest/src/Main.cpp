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

#include <chrono>
#include <iostream>
#include <utility>
#include <Config/ConfigParser.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Signal.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>
#include <SystestExecutor.hpp>
#include <Thread.hpp>
#include <Version.hpp>

int main(int argc, const char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("systest");
        return 0;
    }
    NES::setupSignalHandlers();
    const auto startTime = std::chrono::high_resolution_clock::now();
    NES::Thread::initializeThread(NES::Host("systest"), "main");

    CPPTRACE_TRY
    {
        NES::loadBuiltinPlugins();
        auto config = NES::parseConfig(argc, argv);
        NES::SystestExecutor executor(std::move(config));
        const auto result = executor.executeSystests();

        switch (result.returnType)
        {
            case SystestExecutorResult::ReturnType::SUCCESS: {
                const auto endTime = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
                fmt::print(
                    "{}\nTotal execution time: {} ms ({:.3f} seconds)\n",
                    result.outputMessage,
                    duration.count(),
                    std::chrono::duration_cast<std::chrono::duration<double>>(duration).count());
                return 0;
            }
            case SystestExecutorResult::ReturnType::FAILED: {
                auto endTime = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);
                PRECONDITION(result.errorCode, "Returning with as 'FAILED_WITH_EXCEPTION_CODE', but did not provide error code");
                NES_ERROR("{}", result.outputMessage);
                std::cout << result.outputMessage << '\n';
                std::cout << "Total execution time: " << duration.count() << " ms ("
                          << std::chrono::duration_cast<std::chrono::duration<double>>(duration).count() << " seconds)" << '\n';
                return result.errorCode.value();
            }
        }
    }
    CPPTRACE_CATCH(const NES::Exception&)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
