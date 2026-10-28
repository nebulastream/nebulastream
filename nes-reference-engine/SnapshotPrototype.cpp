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
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <CompositeStatisticListener.hpp>
#include <ModelCatalog.hpp>
#include <QueryCompiler.hpp>
#include <QueryId.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <ReferenceNodeEngine.hpp>
#include "PrototypeQuery.hpp"

/// Pauses a windowed query over a CSV file into an in-memory snapshot, resumes it from the snapshot, and checks that every window
/// appears exactly once with the values of an uninterrupted run.
using namespace NES;
using namespace NES::PrototypeQuery;

int main(int argc, char** argv)
{
    try
    {
        Logger::setupLogging("nes-reference-snapshot-prototype.log", LogLevel::LOG_ERROR);
        loadBuiltinPlugins();
        if (argc < 2 || argc > 3)
        {
            std::cerr << "Usage: nes-reference-snapshot-prototype WORK_DIRECTORY [ROWS]\n";
            return 2;
        }
        const std::filesystem::path directory(argv[1]);
        const uint64_t rows = argc == 3 ? std::stoull(argv[2]) : 2'000'000;
        std::filesystem::create_directories(directory);
        const auto input = directory / "snapshot-prototype-input.csv";
        const auto output = directory / "snapshot-prototype-output.csv";
        std::filesystem::remove(output);
        writeInput(input, rows);

        auto sources = std::make_shared<SourceCatalog>();
        auto sinks = std::make_shared<SinkCatalog>();
        registerCatalog(sources, sinks, fileSource(input), output);
        const RuleBasedOptimizer optimizer(QueryOptimizerConfiguration{}, sources, sinks, std::make_shared<ModelCatalog>());
        WorkerConfiguration configuration;
        if (std::getenv("NES_SNAPSHOT_INTERPRETER") != nullptr)
        {
            configuration.defaultQueryExecution.executionMode.setValue(ExecutionMode::INTERPRETER);
        }
        QueryCompilation::QueryCompiler compiler(configuration.defaultQueryExecution);
        const auto queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
        std::optional<LogicalPlan> donorPlan;
        const auto expected = expectedWindows(rows);

        size_t windowsBeforePause = 0;
        {
            ReferenceNodeEngine engine(configuration, std::make_shared<CompositeStatisticListener>(), Host{"localhost"});
            engine.startQuery(queryId, compile(WINDOW_QUERY, queryId, optimizer, compiler, donorPlan));
            waitForOutput(output, std::chrono::seconds(30), [](const Windows& windows) { return not windows.empty(); });

            const auto snapshot = engine.pauseQuery(queryId);
            windowsBeforePause = readWindows(output).size();
            if (windowsBeforePause >= expected.size())
            {
                throw std::runtime_error("The query finished before the pause; increase ROWS");
            }
            /// Paused: nothing may be produced until the query resumes.
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            if (readWindows(output).size() != windowsBeforePause)
            {
                throw std::runtime_error("Paused query kept producing output");
            }

            engine.resumeQuery(queryId, compile(WINDOW_QUERY, queryId, optimizer, compiler, donorPlan), *snapshot);
            waitForOutput(
                output, std::chrono::seconds(120), [&](const Windows& windows) { return windows.size() >= expected.size(); });
            engine.stopQuery(queryId);
        }

        verifyWindows(readWindows(output), expected);
        std::cout << "Paused after " << windowsBeforePause << " of " << expected.size()
                  << " windows, resumed from the snapshot, and verified every window exactly once\n";
        return 0;
    }
    catch (const std::exception& error)
    {
        std::cerr << "Snapshot prototype failed: " << error.what() << '\n';
        return 1;
    }
}
