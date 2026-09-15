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

#include <cpptrace/from_current.hpp>

#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <ErrorHandling.hpp>
#include <Version.hpp>

extern "C" int nebucli_main();

/// The Rust side parses the arguments, so it calls back here once it sees the debug flag.
extern "C" void nebucli_enable_debug_logging()
{
    NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
}

int main(const int argc, char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("nes-cli");
        return 0;
    }
    CPPTRACE_TRY
    {
        /// Also receives the logs of the Rust code.
        NES::Logger::setupLogging("nes-cli.log", NES::LogLevel::LOG_ERROR, false);
        NES::loadBuiltinPlugins();
        return nebucli_main();
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
