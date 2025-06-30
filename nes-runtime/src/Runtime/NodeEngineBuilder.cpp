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

#include <Runtime/NodeEngineBuilder.hpp>

#include <chrono>
#include <memory>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Listeners/QueryLog.hpp>
#include <Listeners/SystemEventListener.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <QueryEngine.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(
    const NES::Configurations::WorkerConfiguration& workerConfiguration,
    std::shared_ptr<SystemEventListener> systemEventListener,
    std::vector<std::shared_ptr<QueryEngineStatisticListener>> statisticEventListener)
    : workerConfiguration(workerConfiguration)
    , systemEventListener(std::move(std::move(systemEventListener)))
    , statisticEventListener(std::move(std::move(statisticEventListener)))
{
}


std::unique_ptr<NodeEngine> NodeEngineBuilder::build()
{
    auto bufferManager = Memory::BufferManager::create(
        workerConfiguration.bufferSizeInBytes.getValue(), workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue());
    auto queryLog = std::make_shared<QueryLog>();


    auto queryEngine
        = std::make_unique<QueryEngine>(workerConfiguration.queryEngineConfiguration, statisticEventListener, queryLog, bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager),
        systemEventListener,
        std::move(queryLog),
        std::move(queryEngine),
        workerConfiguration.numberOfBuffersInSourceLocalPools.getValue());
}

}
