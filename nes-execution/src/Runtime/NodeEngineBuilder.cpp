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

#include <memory>
#include <utility>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

NodeEngineBuilder::NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration)
    : workerConfiguration(workerConfiguration)
{
}

NodeEngineBuilder& NodeEngineBuilder::setQueryManager(std::unique_ptr<QueryEngine> queryManager)
{
    this->queryManager = std::move(queryManager);
    return *this;
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build()
{
    auto bufferManager = Memory::BufferManager::create(
        workerConfiguration.bufferSizeInBytes.getValue(), workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue());
    auto queryLog = std::make_shared<QueryLog>();
    if (!this->queryManager)
    {
        std::vector bufferManagers = {bufferManager};
        queryManager = std::make_unique<QueryEngine>(2, queryLog, bufferManagers[0]);
    }

    return std::make_unique<NodeEngine>(
        Sources::SourceProvider::create(), std::move(bufferManager), std::move(queryLog), std::move(queryManager));
}

}
