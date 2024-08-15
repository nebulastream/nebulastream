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
#include <Exceptions/SignalHandling.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

NodeEngineBuilder::NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration)
    : workerConfiguration(workerConfiguration)
{
}

NodeEngineBuilder& NodeEngineBuilder::setBufferManagers(std::vector<Memory::BufferManagerPtr> bufferManagers)
{
    this->bufferManagers = std::move(bufferManagers);
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setQueryManager(QueryManagerPtr queryManager)
{
    this->queryManager = std::move(queryManager);
    return *this;
}

std::unique_ptr<NodeEngine> NodeEngineBuilder::build()
{
    try
    {
        std::vector<Memory::BufferManagerPtr> bufferManagers;

        ///create buffer manager

        bufferManagers.push_back(Memory::BufferManager::create(
            workerConfiguration.bufferSizeInBytes.getValue(), workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue()));

        if (bufferManagers.empty())
        {
            NES_ERROR("Runtime: error while building NodeEngine: no BufferManager provided");
            throw Exceptions::RuntimeException(
                "Error while building NodeEngine : no BufferManager provided", NES::collectAndPrintStacktrace());
        }

        auto queryLog = std::make_shared<QueryLog>();

        QueryManagerPtr queryManager{this->queryManager};
        if (!this->queryManager)
        {
            auto numOfThreads = static_cast<uint16_t>(workerConfiguration.numberOfWorkerThreads.getValue());
            std::vector<uint64_t> workerToCoreMappingVec = NES::Util::splitWithStringDelimiter<uint64_t>("", ",");
            queryManager = std::make_shared<QueryManager>(queryLog, bufferManagers, WorkerId(0), numOfThreads, 100, workerToCoreMappingVec);
        }
        if (!queryManager)
        {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating QueryManager");
            throw Exceptions::RuntimeException(
                "Error while building NodeEngine : Error while creating QueryManager", NES::collectAndPrintStacktrace());
        }

        return std::make_unique<NodeEngine>(std::move(bufferManagers), std::move(queryManager), std::move(queryLog));
    }
    catch (std::exception& err)
    {
        NES_ERROR("Cannot start node engine {}", err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
}

} ///namespace NES::Runtime
