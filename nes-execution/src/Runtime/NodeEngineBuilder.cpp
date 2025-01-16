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
#include <memory>

namespace NES::Runtime {

NodeEngineBuilder::NodeEngineBuilder(Configurations::WorkerConfigurationPtr workerConfiguration)
    : workerConfiguration(workerConfiguration) {}

NodeEngineBuilder NodeEngineBuilder::create(Configurations::WorkerConfigurationPtr workerConfiguration) {
    return NodeEngineBuilder(workerConfiguration);
}

NodeEngineBuilder& NodeEngineBuilder::setQueryStatusListener(AbstractQueryStatusListenerPtr nesWorker) {
    this->nesWorker = std::move(nesWorker);
    return *this;
};

NodeEngineBuilder& NodeEngineBuilder::setWorkerId(WorkerId nodeEngineId) {
    this->nodeEngineId = nodeEngineId;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setPartitionManager(Network::PartitionManagerPtr partitionManager) {
    this->partitionManager = partitionManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setHardwareManager(HardwareManagerPtr hardwareManager) {
    this->hardwareManager = hardwareManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setBufferManagers(std::vector<BufferManagerPtr> bufferManagers) {
    this->bufferManagers = bufferManagers;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setQueryManager(QueryManagerPtr queryManager) {
    this->queryManager = queryManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setLanguageCompiler(std::shared_ptr<Compiler::LanguageCompiler> languageCompiler) {
    this->languageCompiler = languageCompiler;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setJITCompiler(Compiler::JITCompilerPtr jitCompiler) {
    this->jitCompiler = jitCompiler;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setPhaseFactory(QueryCompilation::Phases::PhaseFactoryPtr phaseFactory) {
    this->phaseFactory = phaseFactory;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setOpenCLManager(NES::Runtime::OpenCLManagerPtr openCLManager) {
    this->openCLManager = openCLManager;
    return *this;
}

NES::Runtime::NodeEnginePtr NodeEngineBuilder::build() {
    NES_ASSERT(nesWorker, "NesWorker is null");
    try {
        auto nodeEngineId = (this->nodeEngineId == INVALID<WorkerId>) ? getNextWorkerId() : this->nodeEngineId;
        auto partitionManager =
            (!this->partitionManager) ? std::make_shared<Network::PartitionManager>() : this->partitionManager;
        auto hardwareManager = (!this->hardwareManager) ? std::make_shared<Runtime::HardwareManager>() : this->hardwareManager;
        std::vector<BufferManagerPtr> bufferManagers;

        //get the list of queue where to pin from the config
        auto numberOfQueues = workerConfiguration->numberOfQueues.getValue();

        //create one buffer manager per queue
        if (numberOfQueues == 1) {
            bufferManagers.push_back(
                std::make_shared<BufferManager>(workerConfiguration->bufferSizeInBytes.getValue(),
                                                workerConfiguration->numberOfBuffersInGlobalBufferManager.getValue(),
                                                hardwareManager->getGlobalAllocator()));
        } else {
            for (auto i = 0u; i < numberOfQueues; ++i) {
                bufferManagers.push_back(std::make_shared<BufferManager>(
                    workerConfiguration->bufferSizeInBytes.getValue(),
                    //if we run in static with multiple queues, we divide the whole buffer manager among the queues
                    workerConfiguration->numberOfBuffersInGlobalBufferManager.getValue() / numberOfQueues,
                    hardwareManager->getGlobalAllocator()));
            }
        }

        if (bufferManagers.empty()) {
            NES_ERROR("Runtime: error while building NodeEngine: no NesWorker provided");
            throw Exceptions::RuntimeException("Error while building NodeEngine : no NesWorker provided",
                                               NES::collectAndPrintStacktrace());
        }

        QueryManagerPtr queryManager{this->queryManager};
        if (!this->queryManager) {
            auto numOfThreads = static_cast<uint16_t>(workerConfiguration->numWorkerThreads.getValue());
            auto numberOfBuffersPerEpoch = static_cast<uint16_t>(workerConfiguration->numberOfBuffersPerEpoch.getValue());
            std::vector<uint64_t> workerToCoreMappingVec =
                NES::Util::splitWithStringDelimiter<uint64_t>(workerConfiguration->workerPinList.getValue(), ",");
            switch (workerConfiguration->queryManagerMode.getValue()) {
                case QueryExecutionMode::Dynamic: {
                    queryManager = std::make_shared<DynamicQueryManager>(nesWorker,
                                                                         bufferManagers,
                                                                         nodeEngineId,
                                                                         numOfThreads,
                                                                         hardwareManager,
                                                                         numberOfBuffersPerEpoch,
                                                                         workerToCoreMappingVec);
                    break;
                }
                case QueryExecutionMode::Static: {
                    queryManager =
                        std::make_shared<MultiQueueQueryManager>(nesWorker,
                                                                 bufferManagers,
                                                                 nodeEngineId,
                                                                 numOfThreads,
                                                                 hardwareManager,
                                                                 numberOfBuffersPerEpoch,
                                                                 workerToCoreMappingVec,
                                                                 workerConfiguration->numberOfQueues.getValue(),
                                                                 workerConfiguration->numberOfThreadsPerQueue.getValue());
                    break;
                }
                default: {
                    NES_ASSERT(false, "Cannot build Query Manager");
                }
            }
        }
        if (!partitionManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating PartitionManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating PartitionManager",
                                               NES::collectAndPrintStacktrace());
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating QueryManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating QueryManager",
                                               NES::collectAndPrintStacktrace());
        }

        auto queryCompilationOptions = createQueryCompilationOptions(workerConfiguration->queryCompiler, workerConfiguration->numberOfBuffersInSourceLocalBufferPool / 2 + 50);
        queryCompilationOptions->numOfBuffersToProduce = workerConfiguration->numberOfBuffersToProduce;
        auto phaseFactory = (!this->phaseFactory) ? QueryCompilation::Phases::DefaultPhaseFactory::create() : this->phaseFactory;
        auto operatorHandlerStore = OperatorHandlerStore::create();

        auto compiler = QueryCompilation::NautilusQueryCompiler::create(queryCompilationOptions,
                                                                        phaseFactory,
                                                                        operatorHandlerStore,
                                                                        workerConfiguration->enableSourceSharing.getValue());

        if (!compiler) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating compiler");
            throw Exceptions::RuntimeException("Error while building NodeEngine : failed to create compiler",
                                               NES::collectAndPrintStacktrace());
        }
        std::vector<PhysicalSourceTypePtr> physicalSources;
        for (auto entry : workerConfiguration->physicalSourceTypes.getValues()) {
            physicalSources.push_back(entry.getValue());
        }
        if (!openCLManager) {
            NES_DEBUG("Creating default OpenCLManager");
            openCLManager = std::make_shared<OpenCLManager>();
        }
        std::shared_ptr<NodeEngine> engine = std::make_shared<NodeEngine>(
            physicalSources,
            std::move(hardwareManager),
            std::move(bufferManagers),
            std::move(queryManager),
            [this](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(engine->getWorkerId(),
                                                       this->workerConfiguration->localWorkerHost.getValue(),
                                                       this->workerConfiguration->dataPort.getValue(),
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       this->workerConfiguration->senderHighwatermark.getValue(),
                                                       this->workerConfiguration->numWorkerThreads.getValue(),
                                                       workerConfiguration->connectSinksAsync.getValue(),
                                                       workerConfiguration->connectSourceEventChannelsAsync.getValue());
            },
            std::move(partitionManager),
            std::move(operatorHandlerStore),
            std::move(compiler),
            std::move(nesWorker),
            std::move(openCLManager),
            nodeEngineId,
            workerConfiguration->numberOfBuffersInGlobalBufferManager.getValue(),
            workerConfiguration->numberOfBuffersInSourceLocalBufferPool.getValue(),
            workerConfiguration->numberOfBuffersPerWorker.getValue(),
            workerConfiguration->enableSourceSharing.getValue());
        engine->setNumberOfBuffersToProduce(workerConfiguration->numberOfBuffersToProduce.getValue());
        //        Exceptions::installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine {}", err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
}

QueryCompilation::QueryCompilerOptionsPtr
NodeEngineBuilder::createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration& queryCompilerConfiguration, uint64_t numOfBuffers) {
    auto queryCompilerType = queryCompilerConfiguration.queryCompilerType;
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();

    // set compilation mode
    queryCompilationOptions->setCompilationStrategy(queryCompilerConfiguration.compilationStrategy);

    // set pipelining strategy mode
    queryCompilationOptions->setPipeliningStrategy(queryCompilerConfiguration.pipeliningStrategy);

    // set output buffer optimization level
    queryCompilationOptions->setOutputBufferOptimizationLevel(queryCompilerConfiguration.outputBufferOptimizationLevel);

    queryCompilationOptions->setNumSourceLocalBuffers(numOfBuffers);

    if (queryCompilerType == QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER
        && queryCompilerConfiguration.windowingStrategy == QueryCompilation::WindowingStrategy::LEGACY) {
        // sets SLICING windowing strategy as the default if nautilus is active.
        NES_WARNING("The LEGACY window strategy is not supported by Nautilus. Switch to SLICING!")
        queryCompilationOptions->setWindowingStrategy(QueryCompilation::WindowingStrategy::SLICING);
    } else {
        // sets the windowing strategy
        queryCompilationOptions->setWindowingStrategy(queryCompilerConfiguration.windowingStrategy);
    }

    // sets the query compiler
    queryCompilationOptions->setQueryCompiler(queryCompilerConfiguration.queryCompilerType);

    // set the dump mode
    queryCompilationOptions->setDumpMode(queryCompilerConfiguration.queryCompilerDumpMode);

    // set nautilus backend
    queryCompilationOptions->setNautilusBackend(queryCompilerConfiguration.nautilusBackend);

    queryCompilationOptions->getHashJoinOptions()->setNumberOfPartitions(
        queryCompilerConfiguration.numberOfPartitions.getValue());
    queryCompilationOptions->getHashJoinOptions()->setPageSize(queryCompilerConfiguration.pageSize.getValue());
    queryCompilationOptions->getHashJoinOptions()->setPreAllocPageCnt(queryCompilerConfiguration.preAllocPageCnt.getValue());
    //zero indicate that it has not been set in the yaml config
    if (queryCompilerConfiguration.maxHashTableSize.getValue() != 0) {
        queryCompilationOptions->getHashJoinOptions()->setTotalSizeForDataStructures(
            queryCompilerConfiguration.maxHashTableSize.getValue());
    }

    queryCompilationOptions->setStreamJoinStrategy(queryCompilerConfiguration.joinStrategy);

    queryCompilationOptions->setCUDASdkPath(queryCompilerConfiguration.cudaSdkPath.getValue());

    return queryCompilationOptions;
}

WorkerId NodeEngineBuilder::getNextWorkerId() {
    const uint64_t max = -1;
    uint64_t id = time(nullptr) ^ getpid();
    return WorkerId(++id % max);
}
}//namespace NES::Runtime
