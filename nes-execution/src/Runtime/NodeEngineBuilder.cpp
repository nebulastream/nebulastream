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
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <utility>
#include <variant>
#include <vector>
#include <Configuration/WorkerConfiguration.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <QueryEngine.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES::Runtime
{

template <typename T>
const char* functionName()
{
#ifdef _MSC_VER
    return __FUNCSIG__;
#else
    return __PRETTY_FUNCTION__;
#endif
}


struct PrintingStatisticListener final : QueryEngineStatisticListener
{
    void onEvent(Event event) override { events.writeIfNotFull(event); }

    explicit PrintingStatisticListener(const std::filesystem::path& path)
        : file(path, std::ios::out | std::ios::app)
        , printThread([this](const std::stop_token& stopToken) { this->threadRoutine(stopToken); })
    {
        NES_INFO("Writing Statistics to: {}", path);
    }

private:
    void threadRoutine(const std::stop_token& token)
    {
        while (!token.stop_requested())
        {
            Event event = QueryStart{WorkerThreadId(0), QueryId(0)};

            if (!events.tryReadUntil(std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(100), event))
            {
                continue;
            }
            std::visit(
                Overloaded{
                    [&](TaskExecutionStart taskStartEvent)
                    {
                        file << fmt::format(
                            "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Started with no. of tuples: {}\n",
                            taskStartEvent.timestamp,
                            taskStartEvent.taskId,
                            taskStartEvent.pipelineId,
                            taskStartEvent.queryId,
                            taskStartEvent.numberOfTuples);
                        file.flush();
                    },
                    [&](TaskExecutionComplete taskStopEvent)
                    {
                        file << fmt::format(
                            "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Completed.\n",
                            taskStopEvent.timestamp,
                            taskStopEvent.id,
                            taskStopEvent.pipelineId,
                            taskStopEvent.queryId);
                        file.flush();
                    },
                    [](auto) {}},
                event);
        }
    }

    std::ofstream file;
    folly::MPMCQueue<Event> events{100};
    std::jthread printThread;
};

NodeEngineBuilder::NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration)
    : workerConfiguration(workerConfiguration)
{
}


std::unique_ptr<NodeEngine> NodeEngineBuilder::build()
{
    auto bufferManager = Memory::BufferManager::create(
        workerConfiguration.bufferSizeInBytes.getValue(), workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue());
    auto queryLog = std::make_shared<QueryLog>();
    const auto pid = ::getpid();
    const auto timestamp = std::chrono::system_clock::now();

    std::vector bufferManagers = {bufferManager};
    auto queryManager = std::make_unique<QueryEngine>(
        workerConfiguration.queryEngineConfiguration,
        std::make_shared<PrintingStatisticListener>(std::filesystem::path("statistics.txt")),
        queryLog,
        bufferManagers[0]);

    return std::make_unique<NodeEngine>(std::move(bufferManager), std::move(queryLog), std::move(queryManager));
}

}
