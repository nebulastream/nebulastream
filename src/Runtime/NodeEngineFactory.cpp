/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Runtime/NodeEngineFactory.hpp>
#include <Util/Logger.hpp>
namespace NES::Runtime {
NodeEnginePtr NodeEngineFactory::createDefaultNodeEngine(const std::string& hostname,
                                                                       uint16_t port,
                                                                       PhysicalStreamConfigPtr config) {
    return createNodeEngine(hostname, port, std::move(config), 1, 4096, 1024, 128, 12);
}

NodeEnginePtr NodeEngineFactory::createNodeEngine(const std::string& hostname,
                                                  const uint16_t port,
                                                  const PhysicalStreamConfigPtr config,
                                                  const uint16_t numThreads,
                                                  const uint64_t bufferSize,
                                                  const uint64_t numberOfBuffersInGlobalBufferManager,
                                                  const uint64_t numberOfBuffersInSourceLocalBufferPool,
                                                  const uint64_t numberOfBuffersPerPipeline) {

    try {
        auto nodeEngineId = UtilityFunctions::getNextNodeEngineId();
        auto partitionManager = std::make_shared<Network::PartitionManager>();
        auto bufferManager = std::make_shared<BufferManager>(bufferSize, numberOfBuffersInGlobalBufferManager);
        auto queryManager = std::make_shared<QueryManager>(bufferManager, nodeEngineId, numThreads);
        auto stateManager = std::make_shared<StateManager>(nodeEngineId);
        if (!partitionManager) {
            NES_ERROR("Runtime: error while creating partition manager");
            throw Exception("Error while creating partition manager");
        }
        if (!bufferManager) {
            NES_ERROR("Runtime: error while creating buffer manager");
            throw Exception("Error while creating buffer manager");
        }
        if (!queryManager) {
            NES_ERROR("Runtime: error while creating queryManager");
            throw Exception("Error while creating queryManager");
        }
        if (!stateManager) {
            NES_ERROR("Runtime: error while creating stateManager");
            throw Exception("Error while creating stateManager");
        }
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto phaseFactory = QueryCompilation::Phases::DefaultPhaseFactory::create();
        auto compiler = QueryCompilation::DefaultQueryCompiler::create(compilerOptions, phaseFactory, jitCompiler);
        if (!compiler) {
            NES_ERROR("Runtime: error while creating compiler");
            throw Exception("Error while creating compiler");
        }
        auto engine = std::make_shared<NodeEngine>(
            config,
            std::move(bufferManager),
            std::move(queryManager),
            [hostname, port, numThreads](const std::shared_ptr<NodeEngine>& engine) {
                return Network::NetworkManager::create(hostname,
                                                       port,
                                                       Network::ExchangeProtocol(engine->getPartitionManager(), engine),
                                                       engine->getBufferManager(),
                                                       numThreads);
                },
                std::move(partitionManager),
                std::move(compiler),
                std::move(stateManager),
                nodeEngineId,
                numberOfBuffersInGlobalBufferManager,
                numberOfBuffersInSourceLocalBufferPool,
                numberOfBuffersPerPipeline);
        installGlobalErrorListener(engine);
        return engine;
    } catch (std::exception& err) {
        NES_ERROR("Cannot start node engine " << err.what());
        NES_THROW_RUNTIME_ERROR("Cant start node engine");
    }
    return nullptr;
}

}// namespace NES::Runtime