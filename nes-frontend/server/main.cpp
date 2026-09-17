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

#include <algorithm>
#include <cstddef>
#include <span>
#include <string_view>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <Thread.hpp>
#include <Version.hpp>
#include <scope_guard.hpp>

extern "C" int nes_server_main();

namespace
{
bool hasDebugFlag(const std::span<char*> args)
{
    return std::ranges::any_of(
        args,
        [](const char* arg)
        {
            const std::string_view flag{arg};
            return flag == "-d" || flag == "--debug";
        });
}
}

int main(const int argc, char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("nes-server");
        return 0;
    }
    NES::setupSignalHandlers();
    NES::Thread::initializeThread(NES::Host("nes-server"), "main");
    const auto logLevel = hasDebugFlag({argv, static_cast<size_t>(argc)}) ? NES::LogLevel::LOG_DEBUG : NES::LogLevel::LOG_INFO;
    NES::Logger::setupLogging("nes-server.log", logLevel);
    SCOPE_EXIT
    {
        if (const auto logger = NES::Logger::getInstance())
        {
            logger->forceFlush();
        }
    };
    NES::loadBuiltinPlugins();
    return nes_server_main();
}
