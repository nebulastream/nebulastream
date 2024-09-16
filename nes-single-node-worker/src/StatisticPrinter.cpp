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

#include <filesystem>
#include <fstream>
#include <ios>
#include <stop_token>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/chrono.h>
#include <fmt/format.h>
#include <fmt/std.h>
#include <folly/MPMCQueue.h>
#include <QueryEngineStatisticListener.hpp>
#include <StatisticPrinter.hpp>

namespace NES::Runtime
{

namespace
{
void threadRoutine(
    const std::stop_token& token, std::ofstream& file, folly::MPMCQueue<PrintingStatisticListener::CombinedEventType>& events)
{
    while (!token.stop_requested())
    {
        PrintingStatisticListener::CombinedEventType event = QueryStart{WorkerThreadId(0), QueryId(0)}; /// Will be overwritten

        if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
        {
            continue;
        }
        std::visit(
            Overloaded{
                [&](SubmitQuerySystemEvent startQuery)
                {
                    file << fmt::format(
                        "{} Submit Query {}:\n{}\n", startQuery.timestamp.time_since_epoch().count(), startQuery.queryId, startQuery.query);
                },
                [&](StartQuerySystemEvent startQuery)
                { file << fmt::format("{} Start Query {}\n", startQuery.timestamp, startQuery.queryId); },
                [&](TaskExecutionStart taskStartEvent)
                {
                    file << fmt::format(
                        "{} Task {} for Pipeline {} of Query {} Started. Number of Tuples: {}\n",
                        taskStartEvent.timestamp,
                        taskStartEvent.taskId,
                        taskStartEvent.pipelineId,
                        taskStartEvent.queryId,
                        taskStartEvent.numberOfTuples);
                },
                [&](TaskExecutionComplete taskStopEvent)
                {
                    file << fmt::format(
                        "{} Task {} for Pipeline {} of Query {} Completed\n",
                        taskStopEvent.timestamp,
                        taskStopEvent.taskId,
                        taskStopEvent.pipelineId,
                        taskStopEvent.queryId);
                },
                [](auto) {}},
            event);
    }
}
}

void PrintingStatisticListener::onEvent(Event event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

void PrintingStatisticListener::onEvent(SystemEvent event)
{
    events.writeIfNotFull(std::visit([]<typename T>(T&& arg) { return CombinedEventType(std::forward<T>(arg)); }, std::move(event)));
}

PrintingStatisticListener::PrintingStatisticListener(const std::filesystem::path& path)
    : file(path, std::ios::out | std::ios::app)
    , printThread([this](const std::stop_token& stopToken) { threadRoutine(stopToken, file, events); })
{
    NES_INFO("Writing Statistics to: {}", path);
}
}
