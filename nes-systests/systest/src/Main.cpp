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
#include <string>
#include <variant>

#include <cpptrace/from_current.hpp>
#include <fmt/format.h>

#include <Config/ConfigParser.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Signal.hpp>
#include <ErrorHandling.hpp>
#include <Executor.hpp>
#include <Thread.hpp>
#include <Version.hpp>

namespace
{

/// Prints the report of a finished run and returns the exit code.
/// Both outcomes print to standard output, with the time the run took.
/// A failure also writes its report to the log, next to what the individual failures logged as they happened.
int report(const NES::ExecutorResult& result, const std::chrono::steady_clock::duration elapsed)
{
    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    const auto announce = [&](const std::string& outcome)
    {
        fmt::print(
            "{}\nTotal execution time: {} ms ({:.3f} seconds)\n",
            outcome,
            milliseconds.count(),
            std::chrono::duration_cast<std::chrono::duration<double>>(milliseconds).count());
    };

    return std::visit(
        NES::Overloaded{
            [&](const NES::RunSucceeded& succeeded)
            {
                announce(succeeded.report);
                return 0;
            },
            [&](const NES::RunFailed& failed)
            {
                NES_ERROR("{}", failed.report);
                announce(failed.report);
                return static_cast<int>(failed.errorCode);
            }},
        result);
}

}

int main(const int argc, const char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("systest");
        return 0;
    }
    NES::setupSignalHandlers();
    NES::loadBuiltinPlugins();
    NES::Thread::initializeThread(NES::Host("systest"), "main");
    /// A steady clock, so a system time adjustment during a long run cannot distort the reported duration.
    const auto startTime = std::chrono::steady_clock::now();

    CPPTRACE_TRY
    {
        /// A separate statement, because argument evaluation order is unspecified and the clock has to be read after the run.
        const auto result = NES::Executor{NES::parseConfig(argc, argv)}.execute();
        return report(result, std::chrono::steady_clock::now() - startTime);
    }
    CPPTRACE_CATCH(const NES::Exception&)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
