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

#include <StatisticPrinter.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <ios>
#include <ranges>
#include <stop_token>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>

namespace NES::Runtime
{

namespace
{
void handleEvent(PrintingStatisticListener::CombinedEventType event, std::vector<std::ofstream>& files, std::ofstream& systemEventFile)
{
    std::visit(
        Overloaded{
            [&](SubmitQuerySystemEvent startQuery)
            {
                systemEventFile << fmt::format(
                    "{:%Y-%m-%d %H:%M:%S} Submit Query {}:\n{}\n", startQuery.timestamp, startQuery.queryId, startQuery.query);
            },
            [&](StartQuerySystemEvent startQuery)
            { systemEventFile << fmt::format("{:%Y-%m-%d %H:%M:%S} Start Query {}\n", startQuery.timestamp, startQuery.queryId); },
            [&](TaskExecutionStart taskStartEvent)
            {
                const auto workerId = taskStartEvent.threadId;
                const auto filesPos = workerId % files.size();
                files[filesPos] << fmt::format(
                    "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Started. Number of Tuples: {}\n",
                    taskStartEvent.timestamp,
                    taskStartEvent.taskId,
                    taskStartEvent.pipelineId,
                    taskStartEvent.queryId,
                    taskStartEvent.numberOfTuples);
                files[filesPos].flush();
            },
            [&](TaskEmit emitEvent)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} to Pipeline {} of Query {}. Number of Tuples: {}\n",
                        emitEvent.timestamp,
                        emitEvent.taskId,
                        emitEvent.fromPipeline,
                        emitEvent.toPipeline,
                        emitEvent.queryId,
                        emitEvent.numberOfTuples);
                },
                [&](TaskExecutionComplete taskStopEvent)
            {
                const auto workerId = taskStopEvent.threadId;
                const auto filesPos = workerId % files.size();
                files[filesPos] << fmt::format(
                    "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Completed\n",
                    taskStopEvent.timestamp,
                    taskStopEvent.taskId,
                    taskStopEvent.pipelineId,
                    taskStopEvent.queryId);
                files[filesPos].flush();
            },
            [](auto) {}},
        event);
}
}

void PrintingStatisticListener::onEvent(Event event)
{
    handleEvent(
        std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)), files, systemEventFile);
}

void PrintingStatisticListener::onEvent(SystemEvent event)
{
    handleEvent(
        std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)), files, systemEventFile);
}

PrintingStatisticListener::PrintingStatisticListener(
    const Configurations::StringOption& statisticDir, const Configurations::UIntOption& numberOfWorkerThreads)
{
    const auto basePath = std::filesystem::path(statisticDir.getValue());
    const auto systemEventPath = basePath / "system_events.txt";
    const auto numberOfThreads = numberOfWorkerThreads.getValue();
    for (uint64_t i = 0; i < numberOfThreads; ++i)
    {
        const auto curPath = basePath / fmt::format("worker_{}.txt", i);
        NES_DEBUG("Opening file {} for writing query engine statistics", curPath);
        files.emplace_back(curPath, std::ios::out | std::ios::app);
    }
    systemEventFile.open(systemEventPath, std::ios::out | std::ios::app);
}
}
