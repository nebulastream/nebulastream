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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Network/NetworkManager.hpp>
#include <Network/PartitionManager.hpp>
#include <QueryCompiler/DefaultQueryCompiler.hpp>
#include <QueryCompiler/Phases/DefaultPhaseFactory.hpp>
#include <QueryCompiler/QueryCompilerOptions.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/MaterializedViewManager.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/QueryManager.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <log4cxx/helpers/exception.h>
#include <memory>

namespace NES::Runtime {

NodeEnginePtr NodeEngineFactory::createDefaultNodeEngine(const std::string& hostname,
                                                         uint16_t port,
                                                         std::vector<PhysicalSourcePtr> physicalSources,
                                                         std::weak_ptr<NesWorker>&& nesWorker) {
    return createNodeEngine(hostname,
                            port,
                            std::move(physicalSources),
                            1,
                            4096,
                            1024,
                            128,
                            12,
                            Configurations::QueryCompilerConfiguration(),
                            std::move(nesWorker),
                            "");
}

NodeEnginePtr NodeEngineFactory::createNodeEngine(const std::string& hostname,
                                                  const uint16_t port,
                                                  std::vector<PhysicalSourcePtr> physicalSources,
                                                  const uint16_t numThreads,
                                                  const uint64_t bufferSize,
                                                  const uint64_t numberOfBuffersInGlobalBufferManager,
                                                  const uint64_t numberOfBuffersInSourceLocalBufferPool,
                                                  const uint64_t numberOfBuffersPerWorker,
                                                  const Configurations::QueryCompilerConfiguration queryCompilerConfiguration,
                                                  std::weak_ptr<NesWorker>&& nesWorker,
                                                  const std::string& workerToCoreMapping,
                                                  uint64_t numberOfQueues,
                                                  uint64_t numberOfThreadsPerQueue,
                                                  Runtime::QueryExecutionMode queryManagerMode) {

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
        bufferManagers.push_back(std::make_shared<BufferManager>(bufferSize,

                                                                 numberOfBuffersInGlobalBufferManager,
                                                                 hardwareManager->getGlobalAllocator()));
#endif
        if (bufferManagers.empty()) {
            NES_ERROR("Runtime: error while creating buffer manager");
            throw Exceptions::RuntimeException("Error while creating buffer manager");
        }

        auto stateManager = std::make_shared<StateManager>(nodeEngineId);
        if (!stateManager) {
            NES_ERROR("Runtime: error while creating stateManager");
            throw Exceptions::RuntimeException("Error while creating stateManager");
        }

        std::vector<uint64_t> workerToCoreMappingVec;
        QueryManagerPtr queryManager;
        if (workerToCoreMapping != "") {
            workerToCoreMappingVec = Util::splitWithStringDelimiter<uint64_t>(workerToCoreMapping, ",");
            NES_ASSERT(workerToCoreMappingVec.size() == numThreads, " we need one position for each thread in mapping");
        }

        switch (queryManagerMode) {
            case QueryExecutionMode::Dynamic: {
                queryManager = std::make_shared<DynamicQueryManager>(bufferManagers,
                                                                     nodeEngineId,
                                                                     numThreads,
                                                                     hardwareManager,
                                                                     stateManager,
                                                                     workerToCoreMappingVec);
                break;
            }
            case QueryExecutionMode::Static: {
                queryManager = std::make_shared<MultiQueueQueryManager>(bufferManagers,
                                                                        nodeEngineId,
                                                                        numThreads,
                                                                        hardwareManager,
                                                                        stateManager,
                                                                        workerToCoreMappingVec,
                                                                        numberOfQueues,
                                                                        numberOfThreadsPerQueue);
                break;
            }
            default: {
                queryManager = nullptr;
                break;
            }
        }

        auto bufferStorage = std::make_shared<BufferStorage>();
        auto materializedViewManager = std::make_shared<Experimental::MaterializedView::MaterializedViewManager>();
        if (!partitionManager) {
            NES_ERROR("Runtime: error while creating partition manager");
            throw Exceptions::RuntimeException("Error while creating partition manager");
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while creating queryManager");
            throw Exceptions::RuntimeException("Error while creating queryManager");
        }
        if (!bufferStorage) {
            NES_ERROR("Runtime: error while creating bufferStorage");
            throw Exceptions::RuntimeException("Error while creating bufferStorage");
        }
        if (!materializedViewManager) {
            NES_ERROR("Runtime: error while creating materializedViewMananger");
            throw Exceptions::RuntimeException("Error while creating materializedViewMananger");
        }
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto queryCompilationOptions = createQueryCompilationOptions(queryCompilerConfiguration);
        queryCompilationOptions->setNumSourceLocalBuffers(numberOfBuffersInSourceLocalBufferPool);
        auto compiler = QueryCompilation::DefaultQueryCompiler::create(queryCompilationOptions, phaseFactory, jitCompiler);
        if (!compiler) {
            NES_ERROR("Runtime: error while creating compiler");
            throw Exceptions::RuntimeException("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            physicalSources,
            std::move(hardwareManager),
            std::move(bufferManagers),
            std::move(queryManager),
            std::move(bufferStorage),
            [hostname, port, numThreads](const std::shared_ptr<NodeEngine>& engine) {
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
NodeEngineFactory::createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration queryCompilerConfiguration) {
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

uint64_t NodeEngineFactory::getNextNodeEngineId() {
    const uint64_t max = -1;
    std::atomic<uint64_t> id = time(nullptr) ^ getpid();
    return (++id % max);
}

}// namespace NES::Runtime