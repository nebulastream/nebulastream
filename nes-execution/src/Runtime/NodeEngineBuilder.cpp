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
#include <Configurations/Worker/WorkerConfiguration.hpp>
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


struct PrintingStatisticListener : QueryEngineStatisticListener
{
    void onEvent(Event event) override
    {
        std::visit(
            Overloaded{
                [&](TaskExecutionStart event)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Started\n",
                        std::chrono::system_clock::now(),
                        event.taskId,
                        event.pipelineId,
                        event.queryId);
                },
                [&](TaskExecutionComplete event)
                {
                    file << fmt::format(
                        "{:%Y-%m-%d %H:%M:%S} Task {} for Pipeline {} of Query {} Completed\n",
                        std::chrono::system_clock::now(),
                        event.id,
                        event.pipelineId,
                        event.queryId);
                },
                [](auto) {}},
            event);
    }

    explicit PrintingStatisticListener(const std::filesystem::path& path) : file(path, std::ios::out | std::ios::app) { }

private:
    std::ofstream file;
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

    std::vector bufferManagers = {bufferManager};
    auto queryManager = std::make_unique<QueryEngine>(
        workerConfiguration.numberOfWorkerThreads.getValue(),
        std::make_shared<PrintingStatisticListener>("/tmp/statistics.txt"),
        queryLog,
        bufferManagers[0]);

    return std::make_unique<NodeEngine>(std::move(bufferManager), std::move(queryLog), std::move(queryManager));
}

}
