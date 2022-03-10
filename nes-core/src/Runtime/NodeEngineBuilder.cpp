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
#include <Exceptions/SignalHandling.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MaterializedViewManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <memory>
#include <log4cxx/helpers/exception.h>

namespace NES::Runtime {
NodeEngineBuilder NodeEngineBuilder::create() { return NodeEngineBuilder(); }

NodeEngineBuilder& NodeEngineBuilder::setHostname(const std::string& hostname) {
    NodeEngineBuilder::hostname = hostname;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setPort(uint16_t port) {
    NodeEngineBuilder::port = port;
    isPortSet = true;
    return *this;
}

NodeEngineBuilder&
NodeEngineBuilder::setPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources) {
    NodeEngineBuilder::physicalSources = physicalSources;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setNumThreads(uint16_t numThreads) {
    NodeEngineBuilder::numThreads = numThreads;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setBufferSize(uint64_t bufferSize) {
    NodeEngineBuilder::bufferSize = bufferSize;
    return *this;
}

NodeEngineBuilder&
NodeEngineBuilder::setNumberOfBuffersInGlobalBufferManager(uint64_t numberOfBuffersInGlobalBufferManager) {
    NodeEngineBuilder::numberOfBuffersInGlobalBufferManager = numberOfBuffersInGlobalBufferManager;
    return *this;
}

NodeEngineBuilder&
NodeEngineBuilder::setNumberOfBuffersInSourceLocalBufferPool(uint64_t numberOfBuffersInSourceLocalBufferPool) {
    NodeEngineBuilder::numberOfBuffersInSourceLocalBufferPool = numberOfBuffersInSourceLocalBufferPool;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setNumberOfBuffersPerWorker(uint64_t numberOfBuffersPerWorker) {
    NodeEngineBuilder::numberOfBuffersPerWorker = numberOfBuffersPerWorker;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setQueryCompilerConfiguration(
    const NES::Configurations::QueryCompilerConfiguration& queryCompilerConfiguration) {
    NodeEngineBuilder::queryCompilerConfiguration = queryCompilerConfiguration;
    isQueryCompilerConfigurationSet = true;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setNesWorker(const std::weak_ptr<NesWorker>&& nesWorker) {
    NodeEngineBuilder::nesWorker = nesWorker;
    return *this;
}

NodeEngineBuilder&
NodeEngineBuilder::setEnableNumaAwareness(NES::Runtime::NumaAwarenessFlag enableNumaAwareness) {
    NodeEngineBuilder::enableNumaAwareness = enableNumaAwareness;
    isNumaAwarenessFlagSet = true;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setWorkerToCoreMapping(const std::string& workerToCoreMapping) {
    NodeEngineBuilder::workerToCoreMapping = workerToCoreMapping;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setNumberOfQueues(uint64_t numberOfQueues) {
    NodeEngineBuilder::numberOfQueues = numberOfQueues;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setNumberOfThreadsPerQueue(uint64_t numberOfThreadsPerQueue) {
    NodeEngineBuilder::numberOfThreadsPerQueue = numberOfThreadsPerQueue;
    return *this;
}

NodeEngineBuilder&
NodeEngineBuilder::setQueryManagerMode(NES::Runtime::QueryManager::QueryMangerMode queryManagerMode) {
    NodeEngineBuilder::queryManagerMode = queryManagerMode;
    isQueryManagerModeSet = true;
    return *this;
}
NES::Runtime::NodeEnginePtr NodeEngineBuilder::build() {

    if(hostname.empty()){
        NES_ERROR("Runtime::NodeEngineBuilder: Hostname for NodeEngine must be explicitly set");
        throw log4cxx::helpers::Exception("Error while creating NodeEngine: no hostname explicitly set");
    }
    if(!isPortSet){
        NES_ERROR("Runtime::NodeEngineBuilder: Port for NodeEngine must be explicitly set");
        throw log4cxx::helpers::Exception("Error while creating NodeEngine: no port set");
    }
    if(nesWorker.expired()){
        NES_ERROR("Runtime::NodeEngineBuilder: NesWorker for NodeEngine must be explicitly provided");
        throw log4cxx::helpers::Exception("Error while creating NodeEngine: no NesWorker explicitly provided");
    }

   //default initialization of vector is empty and can be passed as such to NodeEngine. No further checks are neccesary here

    if(bufferSize == 0){
        bufferSize = 4096;
    }

    if(numberOfBuffersInGlobalBufferManager == 0){
        numberOfBuffersInGlobalBufferManager = 1024;
    }

    if(numberOfBuffersInSourceLocalBufferPool == 0){
        numberOfBuffersInSourceLocalBufferPool = 128;
    }

    if(numberOfBuffersPerWorker == 0){
        numberOfBuffersPerWorker = 12;
    }

    if(!isQueryCompilerConfigurationSet){
        queryCompilerConfiguration = Configurations::QueryCompilerConfiguration();
    }

    if(!isNumaAwarenessFlagSet){
        enableNumaAwareness = NumaAwarenessFlag::DISABLED;
    }

    if(workerToCoreMapping.empty()){
        workerToCoreMapping = "";
    }

    if(numberOfQueues == 0){
        numberOfQueues = 1;
    }

    if (numberOfThreadsPerQueue == 0){
        numberOfThreadsPerQueue = 1;
    }

    if(!isQueryManagerModeSet){
        queryManagerMode = QueryManager::Dynamic;
    }

    try {
        auto nodeEngineId = getNextNodeEngineId();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto hardwareManager = std::make_shared<Runtime::HardwareManager>();
        std::vector<BufferManagerPtr> bufferManagers;
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
        if (enableNumaAwareness == NumaAwarenessFlag::ENABLED) {
            auto numberOfBufferPerNumaNode = numberOfBuffersInGlobalBufferManager / hardwareManager->getNumberOfNumaRegions();
            NES_ASSERT2_FMT(numberOfBuffersInSourceLocalBufferPool < numberOfBufferPerNumaNode,
                            "The number of buffer for each numa node: " << numberOfBufferPerNumaNode
                                                                        << " is lower than the fixed size pool: "
                                                                        << numberOfBuffersInSourceLocalBufferPool);
            NES_ASSERT2_FMT(numberOfBuffersPerWorker < numberOfBufferPerNumaNode,
                            "The number of buffer for each numa node: "
                                << numberOfBufferPerNumaNode << " is lower than the pipeline pool: " << numberOfBuffersPerWorker);
            for (auto i = 0u; i < hardwareManager->getNumberOfNumaRegions(); ++i) {
                bufferManagers.push_back(
                    std::make_shared<BufferManager>(bufferSize, numberOfBufferPerNumaNode, hardwareManager->getNumaAllactor(i)));
            }
        } else {
            bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,
                                                                     numberOfBuffersInGlobalBufferManager,
                                                                     hardwareManager->getGlobalAllocator()));
        }
#elif defined(NES_USE_ONE_QUEUE_PER_QUERY)
        NES_WARNING("Numa flags " << int(enableNumaAwareness));

        //get the list of queue where to pin from the config
        std::vector<uint64_t> queuePinListMapping = Util::splitWithStringDelimiter<uint64_t>(queuePinList, ",");
        auto numberOfQueues = Util::numberOfUniqueValues(queuePinListMapping);

        //create one buffer manager per queue
        if (numberOfQueues == 0) {
            bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,
                                                                     numberOfBuffersInGlobalBufferManager,
                                                                     hardwareManager->getGlobalAllocator()));
        } else {
            for (auto i = 0u; i < numberOfQueues; ++i) {
                bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,
                                                                         numberOfBuffersInGlobalBufferManager,
                                                                         hardwareManager->getGlobalAllocator()));
            }
        }
#else
        NES_WARNING("Numa flags " << int(enableNumaAwareness) << " are ignored");
        bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,

                                                                 numberOfBuffersInGlobalBufferManager,
                                                                 hardwareManager->getGlobalAllocator()));
#endif
        if (bufferManagers.empty()) {
            NES_ERROR("Runtime: error while creating buffer manager");
            throw log4cxx::helpers::Exception("Error while creating buffer manager");
        }

        QueryManagerPtr queryManager;
        if (workerToCoreMapping != "") {
            std::vector<uint64_t> workerToCoreMappingVec = Util::splitWithStringDelimiter<uint64_t>(workerToCoreMapping, ",");
            NES_ASSERT(workerToCoreMappingVec.size() == numThreads, " we need one position for each thread in mapping");
            queryManager = std::make_shared<QueryManager>(bufferManagers,
                                                          nodeEngineId,
                                                          numThreads,
                                                          hardwareManager,
                                                          workerToCoreMappingVec,
                                                          numberOfQueues,
                                                          numberOfThreadsPerQueue,
                                                          queryManagerMode);
        } else {
            queryManager = std::make_shared<QueryManager>(bufferManagers, nodeEngineId, numThreads, hardwareManager);
        }

        auto stateManager = std::make_shared<StateManager>(nodeEngineId);
        auto bufferStorage = std::make_shared<BufferStorage>();
        auto materializedViewManager = std::make_shared<Experimental::MaterializedView::MaterializedViewManager>();
        if (!partitionManager) {
            NES_ERROR("Runtime: error while creating partition manager");
            throw log4cxx::helpers::Exception("Error while creating partition manager");
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while creating queryManager");
            throw log4cxx::helpers::Exception("Error while creating queryManager");
        }
        if (!stateManager) {
            NES_ERROR("Runtime: error while creating stateManager");
            throw log4cxx::helpers::Exception("Error while creating stateManager");
        }
        if (!bufferStorage) {
            NES_ERROR("Runtime: error while creating bufferStorage");
            throw log4cxx::helpers::Exception("Error while creating bufferStorage");
        }
        if (!materializedViewManager) {
            NES_ERROR("Runtime: error while creating materializedViewMananger");
            throw log4cxx::helpers::Exception("Error while creating materializedViewMananger");
        }
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto queryCompilationOptions = createQueryCompilationOptions(queryCompilerConfiguration);
        queryCompilationOptions->setNumSourceLocalBuffers(numberOfBuffersInSourceLocalBufferPool);
        auto compiler = QueryCompilation::DefaultQueryCompiler::create(queryCompilationOptions, phaseFactory, jitCompiler);
        if (!compiler) {
            NES_ERROR("Runtime: error while creating compiler");
            throw log4cxx::helpers::Exception("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            physicalSources,
            std::move(hardwareManager),
            std::move(bufferManagers),
            std::move(queryManager),
            std::move(bufferStorage),
            [this](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(engine->getNodeEngineId(),
                                                       hostname,
                                                       port,
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       numThreads);
            },
            std::move(partitionManager),
            std::move(compiler),
            std::move(stateManager),
            std::move(nesWorker),
            std::move(materializedViewManager),
            nodeEngineId,
            numberOfBuffersInGlobalBufferManager,
            numberOfBuffersInSourceLocalBufferPool,
            numberOfBuffersPerWorker);
        Exceptions::installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
}

QueryCompilation::QueryCompilerOptionsPtr
NodeEngineBuilder::createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration queryCompilerConfiguration) {
    auto queryCompilationOptions = QueryCompilation::QueryCompilerOptions::createDefaultOptions();

    // set compilation mode
    queryCompilationOptions->setCompilationStrategy(queryCompilerConfiguration.compilationStrategy);

    // set pipelining strategy mode
    queryCompilationOptions->setPipeliningStrategy(queryCompilerConfiguration.pipeliningStrategy);

    // set output buffer optimization level
    queryCompilationOptions->setOutputBufferOptimizationLevel(queryCompilerConfiguration.outputBufferOptimizationLevel);

    // sets the windowing strategy
    queryCompilationOptions->setWindowingStrategy(queryCompilerConfiguration.windowingStrategy);
    return queryCompilationOptions;
}

uint64_t NodeEngineBuilder::getNextNodeEngineId() {
    const uint64_t max = -1;
    std::atomic<uint64_t> id = time(nullptr) ^ getpid();
    return (++id % max);
}
}