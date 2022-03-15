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

namespace NES::Runtime {

NodeEngineBuilder::NodeEngineBuilder(Configurations::WorkerConfiguration workerConfiguration): workerConfiguration(workerConfiguration) {}

NodeEngineBuilder NodeEngineBuilder::create(Configurations::WorkerConfiguration workerConfiguration) {

    return NodeEngineBuilder(workerConfiguration);
}

NodeEngineBuilder& NodeEngineBuilder::setNesWorker(std::weak_ptr<NesWorker>&& nesWorker){
    this->nesWorker = std::move(nesWorker);
    return *this;
};

NodeEngineBuilder& NodeEngineBuilder::setNodeEngineId(uint64_t nodeEngineId) {
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

NodeEngineBuilder& NodeEngineBuilder::setQueryManager(QueryManagerPtr queryManager){
    this->queryManager = queryManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setStateManager(StateManagerPtr stateManager){
    this->stateManager = stateManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setBufferStorage(BufferStoragePtr bufferStorage){
    this->bufferStorage = bufferStorage;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setMaterializedViewManager(NES::Experimental::MaterializedView::MaterializedViewManagerPtr materializedViewManager){
    this->materializedViewManager = materializedViewManager;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setLanguageCompiler(std::shared_ptr<Compiler::LanguageCompiler> languageCompiler){
    this->languageCompiler = languageCompiler;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setJITCompiler(Compiler::JITCompilerPtr jitCompiler){
    this->jitCompiler = jitCompiler;
    return *this;
}

NodeEngineBuilder& NodeEngineBuilder::setPhaseFactory(QueryCompilation::Phases::PhaseFactoryPtr phaseFactory){
    this->phaseFactory = phaseFactory;
    return *this;
}
NodeEngineBuilder& NodeEngineBuilder::setCompiler(QueryCompilation::QueryCompilerPtr queryCompiler){
    this->queryCompiler = queryCompiler;
    return *this;
}

NES::Runtime::NodeEnginePtr NodeEngineBuilder::build() {
    if(nesWorker.expired()){
        NES_ERROR("Runtime: error while building NodeEngine: no NesWorker provided");
        throw Exceptions::RuntimeException("Error while building NodeEngine : no NesWorker provided", NES::collectAndPrintStacktrace());
    }

    try {
        auto nodeEngineId = (this->nodeEngineId == 0) ? getNextNodeEngineId() : this->nodeEngineId;
        auto partitionManager = (!this->partitionManager) ? std::make_shared<Network::PartitionManager>() : this->partitionManager;
        auto hardwareManager = (!this->hardwareManager) ? std::make_shared<Runtime::HardwareManager>() : this->hardwareManager;
        std::vector<BufferManagerPtr> bufferManagers;
#ifdef NES_USE_ONE_QUEUE_PER_NUMA_NODE
            if (workerConfiguration.numaAwareness.getValue()) {
                auto numberOfBufferPerNumaNode = workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue()
                    / hardwareManager->getNumberOfNumaRegions();
                NES_ASSERT2_FMT(workerConfiguration.numberOfBuffersInSourceLocalBufferPool.getValue() < numberOfBufferPerNumaNode,
                                "The number of buffer for each numa node: "
                                    << numberOfBufferPerNumaNode << " is lower than the fixed size pool: "
                                    << workerConfiguration.numberOfBuffersInSourceLocalBufferPool.getValue());
                NES_ASSERT2_FMT(workerConfiguration.numberOfBuffersPerWorker.getValue() < numberOfBufferPerNumaNode,
                                "The number of buffer for each numa node: "
                                    << numberOfBufferPerNumaNode << " is lower than the pipeline pool: "
                                    << workerConfiguration.numberOfBuffersPerWorker.getValue());
                for (auto i = 0u; i < hardwareManager->getNumberOfNumaRegions(); ++i) {
                    bufferManagers.push_back(std::make_shared<BufferManager>(workerConfiguration.bufferSizeInBytes.getValue(),
                                                                             numberOfBufferPerNumaNode,
                                                                             hardwareManager->getNumaAllocator(i)));
                }
            } else {
                bufferManagers.push_back(
                    std::make_shared<BufferManager>(workerConfiguration.bufferSizeInBytes.getValue(),
                                                    workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
                                                    hardwareManager->getGlobalAllocator()));
            }
#elif defined(NES_USE_ONE_QUEUE_PER_QUERY)
        NES_WARNING("Numa flags " << int(!workerConfiguration.numaAwareness.getValue()));

        //get the list of queue where to pin from the config
        std::vector<uint64_t> queuePinListMapping = Util::splitWithStringDelimiter<uint64_t>(workerConfiguration.queuePinList.getValue(), ",");
        auto numberOfQueues = Util::numberOfUniqueValues(queuePinListMapping);

        //create one buffer manager per queue
        if (numberOfQueues == 0) {
            bufferManagers.push_back(std::make_shared<BufferManager>(workerConfiguration.bufferSizeInBytes.getValue(),
                                                                     workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
                                                                     hardwareManager->getGlobalAllocator()));
        } else {
            for (auto i = 0u; i < numberOfQueues; ++i) {
                bufferManagers.push_back(std::make_shared<BufferManager>(workerConfiguration.bufferSizeInBytes.getValue(),
                                                                         workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
                                                                         hardwareManager->getGlobalAllocator()));
            }
        }
#else
        if (!this->bufferManagers.empty()) {
            bufferManagers = this->bufferManagers;
        } else {
            NES_WARNING("Numa flags " << int(!workerConfiguration.numaAwareness.getValue()) << " are ignored");
            bufferManagers.push_back(
                std::make_shared<BufferManager>(workerConfiguration.bufferSizeInBytes.getValue(),

                                                workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
                                                hardwareManager->getGlobalAllocator()));
        }
#endif
        if (bufferManagers.empty()) {
            NES_ERROR("Runtime: error while building NodeEngine: no NesWorker provided");
            throw Exceptions::RuntimeException("Error while building NodeEngine : no NesWorker provided", NES::collectAndPrintStacktrace());
        }

        QueryManagerPtr queryManager;
        if(!this->queryManager) {
            queryManager = this->queryManager;
        }
        else {
            if (workerConfiguration.workerPinList.getValue()!= "") {
                std::vector<uint64_t> workerToCoreMappingVec = Util::splitWithStringDelimiter<uint64_t>(workerConfiguration.workerPinList.getValue(), ",");
                NES_ASSERT(workerToCoreMappingVec.size() == workerConfiguration.numWorkerThreads.getValue(), " we need one position for each thread in mapping");
                queryManager = std::make_shared<QueryManager>(bufferManagers,
                                                              nodeEngineId,
                                                              workerConfiguration.numWorkerThreads.getValue(),     //QueryManager expects uint16_t but this value is uint64_t, expect error
                                                              hardwareManager,
                                                              workerToCoreMappingVec,
                                                              workerConfiguration.numberOfQueues.getValue(),
                                                              workerConfiguration.numberOfThreadsPerQueue.getValue(),
                                                              workerConfiguration.queryManagerMode.getValue());
            } else {
                queryManager = std::make_shared<QueryManager>(bufferManagers, nodeEngineId, workerConfiguration.numWorkerThreads.getValue(), hardwareManager);
            }
        }
        auto stateManager = (!this->stateManager) ? std::make_shared<StateManager>(nodeEngineId) : this->stateManager;
        auto bufferStorage = (!this->bufferStorage) ? std::make_shared<BufferStorage>() : this->bufferStorage;
        auto materializedViewManager = (!this->materializedViewManager) ? std::make_shared<Experimental::MaterializedView::MaterializedViewManager>() : this->materializedViewManager;
        if (!partitionManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating PartitionManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating PartitionManager", NES::collectAndPrintStacktrace());
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating QueryManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating QueryManager", NES::collectAndPrintStacktrace());
        }
        if (!stateManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating StateManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating StateManager", NES::collectAndPrintStacktrace());
        }
        if (!bufferStorage) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating BufferStorage");
            throw Exceptions::RuntimeException("Error while building NodeEngine : Error while creating BufferStorage", NES::collectAndPrintStacktrace());
        }
        if (!materializedViewManager) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating MaterializedViewManager");
            throw Exceptions::RuntimeException("Error while building NodeEngine : error while creating MaterializedViewManager", NES::collectAndPrintStacktrace());
        }
        auto cppCompiler = (!this->languageCompiler) ? Compiler::CPPCompiler::create() : this->languageCompiler;
        auto jitCompiler = (!this->jitCompiler) ? Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build() : this->jitCompiler;
        auto phaseFactory = (!this->phaseFactory) ? QueryCompilation::Phases::DefaultPhaseFactory::create() : this->phaseFactory;
        auto queryCompilationOptions = createQueryCompilationOptions(workerConfiguration.queryCompiler);
        queryCompilationOptions->setNumSourceLocalBuffers(workerConfiguration.numberOfBuffersInSourceLocalBufferPool.getValue());
        auto compiler = (!this->queryCompiler) ? QueryCompilation::DefaultQueryCompiler::create(queryCompilationOptions, phaseFactory, jitCompiler) : this->queryCompiler;
        if (!compiler) {
            NES_ERROR("Runtime: error while building NodeEngine: error while creating compiler");
            throw Exceptions::RuntimeException("Error while building NodeEngine : failed to create compiler", NES::collectAndPrintStacktrace());
        }
        std::vector<PhysicalSourcePtr> physicalSources;
        for(auto entry : workerConfiguration.physicalSources.getValues()) {
            physicalSources.push_back(entry.getValue());
        }
        std::shared_ptr<NodeEngine> engine ( new NodeEngine(
            physicalSources,
            std::move(hardwareManager),
            std::move(bufferManagers),
            std::move(queryManager),
            std::move(bufferStorage),
            [this](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(engine->getNodeEngineId(),
                                                       this->workerConfiguration.localWorkerIp.getValue(),
                                                       this->workerConfiguration.dataPort.getValue(),
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       this->workerConfiguration.numWorkerThreads.getValue());
            },
            std::move(partitionManager),
            std::move(compiler),
            std::move(stateManager),
            std::move(nesWorker),
            std::move(materializedViewManager),
            nodeEngineId,
            workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
            workerConfiguration.numberOfBuffersInSourceLocalBufferPool.getValue(),
            workerConfiguration.numberOfBuffersPerWorker.getValue()));

//        auto engine = std::make_shared<NodeEngine>(
//            physicalSources,
//            std::move(hardwareManager),
//            std::move(bufferManagers),
//            std::move(queryManager),
//            std::move(bufferStorage),
//            [this](const std::shared_ptr<NodeEngine>& engine) {
//                return Network::NetworkManager::create(engine->getNodeEngineId(),
//                                                       this->workerConfiguration.localWorkerIp.getValue(),
//                                                       this->workerConfiguration.dataPort.getValue(),
//                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
//                                                       engine->getBufferManager(),
//                                                       this->workerConfiguration.numWorkerThreads.getValue());
//            },
//            std::move(partitionManager),
//            std::move(compiler),
//            std::move(stateManager),
//            std::move(nesWorker),
//            std::move(materializedViewManager),
//            nodeEngineId,
//            workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue(),
//            workerConfiguration.numberOfBuffersInSourceLocalBufferPool.getValue(),
//            workerConfiguration.numberOfBuffersPerWorker.getValue());
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
    uint64_t id = time(nullptr) ^ getpid();
    return (++id % max);
}
}//namespace NES::Runtime