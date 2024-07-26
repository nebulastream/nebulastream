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
#include <Compiler/LanguageCompiler.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/NautilusQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/OpenCLManager.hpp>
#include <Runtime/QueryManager.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime
{

NodeEngineBuilder::NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration)
    : workerConfiguration(workerConfiguration)
{
}

NodeEngineBuilder& NodeEngineBuilder::setBufferManagers(std::vector<BufferManagerPtr> bufferManagers)
{
    this->bufferManagers = std::move(bufferManagers);
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setQueryManager(QueryManagerPtr queryManager)
{
    this->queryManager = std::move(queryManager);
    return *this;
}

class SimpleQueryStatusListener : public AbstractQueryStatusListener
{
public:
    bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryPlanId, OperatorId, Runtime::QueryTerminationType) override { return true; }
    bool notifySourceTermination(SharedQueryId, DecomposedQueryPlanId, OperatorId, Runtime::QueryTerminationType) override
    {
        NES_INFO("Source has terminated");
        return true;
    }
    bool notifyQueryFailure(SharedQueryId, DecomposedQueryPlanId, std::string errorMsg) override
    {
        NES_FATAL_ERROR("Query Failure: {}", errorMsg);
        return true;
    }
    bool notifyQueryStatusChange(SharedQueryId, DecomposedQueryPlanId, Runtime::Execution::ExecutableQueryPlanStatus) override
    {
        return true;
    }
    bool notifyEpochTermination(uint64_t, uint64_t) override { return true; }
};

std::unique_ptr<NodeEngine> NodeEngineBuilder::build()
{
    try
    {
        std::vector<BufferManagerPtr> bufferManagers;

        ///get the list of queue where to pin from the config
        auto numberOfQueues = workerConfiguration.numberOfQueues.getValue();

        ///create one buffer manager per queue
        if (numberOfQueues == 1)
        {
            bufferManagers.push_back(std::make_shared<BufferManager>(
                workerConfiguration.bufferSizeInBytes.getValue(), workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue()));
        }
        else
        {
            for (auto i = 0u; i < numberOfQueues; ++i)
            {
                bufferManagers.push_back(std::make_shared<BufferManager>(
                    workerConfiguration.bufferSizeInBytes.getValue(),
                    ///if we run in static with multiple queues, we divide the whole buffer manager among the queues
                    workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue() / numberOfQueues));
            }
        }

        if (bufferManagers.empty())
        {
            NES_ERROR("Runtime: error while building NodeEngine: no BufferManager provided");
            throw Exceptions::RuntimeException(
                "Error while building NodeEngine : no BufferManager provided", NES::collectAndPrintStacktrace());
        }

        QueryManagerPtr queryManager{this->queryManager};
        if (!this->queryManager)
        {
            auto numOfThreads = static_cast<uint16_t>(workerConfiguration.numWorkerThreads.getValue());
            auto numberOfBuffersPerEpoch = static_cast<uint16_t>(workerConfiguration.numberOfBuffersPerEpoch.getValue());
            std::vector<uint64_t> workerToCoreMappingVec
                = NES::Util::splitWithStringDelimiter<uint64_t>(workerConfiguration.workerPinList.getValue(), ",");
            switch (workerConfiguration.queryManagerMode.getValue())
            {
                case QueryExecutionMode::Dynamic: {
                    queryManager = std::make_shared<DynamicQueryManager>(
                        std::make_shared<SimpleQueryStatusListener>(),
                        bufferManagers,
                        WorkerId(0),
                        numOfThreads,
                        nullptr,
                        numberOfBuffersPerEpoch,
                        workerToCoreMappingVec);
                    break;
                }
                case QueryExecutionMode::Static: {
                    queryManager = std::make_shared<MultiQueueQueryManager>(
                        nullptr,
                        bufferManagers,
                        WorkerId(0),
                        numOfThreads,
                        nullptr,
                        numberOfBuffersPerEpoch,
                        workerToCoreMappingVec,
                        workerConfiguration.numberOfQueues.getValue(),
                        workerConfiguration.numberOfThreadsPerQueue.getValue());
                    break;
                }
                default: {
                    NES_ASSERT(false, "Cannot build Query Manager");
                }
            }
        }
        if (!queryManager)
        {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating QueryManager");
            throw Exceptions::RuntimeException(
                "Error while building NodeEngine : Error while creating QueryManager", NES::collectAndPrintStacktrace());
        }

        return std::make_unique<NodeEngine>(std::move(bufferManagers), std::move(queryManager));
    }
    catch (std::exception& err)
    {
        NES_ERROR("Cannot start node engine {}", err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
}

} ///namespace NES::Runtime
