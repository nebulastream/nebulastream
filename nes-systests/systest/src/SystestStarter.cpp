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
#include <utility>
#include <variant>
#include <Config/ConfigParser.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Signal.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/base.h>
#include <ErrorHandling.hpp>
#include <Executor.hpp>
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
        const NES::Executor executor{NES::parseConfig(argc, argv)};
        const auto result = executor.execute();

        const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now() - startTime);
        return std::visit(
            NES::Overloaded{
                [&](const NES::RunSucceeded& succeeded)
                {
                    fmt::print(
                        "{}\nTotal execution time: {} ms ({:.3f} seconds)\n",
                        succeeded.report,
                        duration.count(),
                        std::chrono::duration_cast<std::chrono::duration<double>>(duration).count());
                    return 0;
                },
                [&](const NES::RunFailed& failed)
                {
                    NES_ERROR("{}", failed.report);
                    fmt::print(
                        "{}\nTotal execution time: {} ms ({:.3f} seconds)\n",
                        failed.report,
                        duration.count(),
                        std::chrono::duration_cast<std::chrono::duration<double>>(duration).count());
                    return static_cast<int>(failed.errorCode);
                }},
            result);
    }
    CPPTRACE_CATCH(const NES::Exception&)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
