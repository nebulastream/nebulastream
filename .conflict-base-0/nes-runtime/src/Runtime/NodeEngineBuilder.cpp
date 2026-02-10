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

#include <memory>
#include <utility>
#include <Configuration/WorkerConfiguration.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceProvider.hpp>
#include <QueryEngine.hpp>

namespace NES
{


NodeEngineBuilder::NodeEngineBuilder(const WorkerConfiguration& workerConfiguration, std::shared_ptr<StatisticListener> statisticsListener)
    : workerConfiguration(workerConfiguration), statisticsListener(std::move(statisticsListener))
{
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build(WorkerId workerId)
{
    auto bufferManager = BufferManager::create(
        workerConfiguration.defaultQueryExecution.operatorBufferSize.getValue(),
        workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue());
    auto queryLog = std::make_shared<QueryLog>();

    auto queryEngine
        = std::make_unique<QueryEngine>(workerConfiguration.queryEngine, statisticsListener, queryLog, bufferManager, workerId);

    auto sourceProvider = std::make_unique<SourceProvider>(workerConfiguration.defaultMaxInflightBuffers.getValue(), bufferManager);

    return std::make_unique<NodeEngine>(
        std::move(bufferManager), statisticsListener, std::move(queryLog), std::move(queryEngine), std::move(sourceProvider));
}

}
