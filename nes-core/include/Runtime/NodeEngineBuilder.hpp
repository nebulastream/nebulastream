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

#ifndef NES_NODEENGINEBUILDER_HPP
#define NES_NODEENGINEBUILDER_HPP
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Compiler/LanguageCompiler.hpp>
namespace NES::Runtime {
enum class NumaAwarenessFlag { ENABLED, DISABLED };
/**
 * This class is used to create instances of NodeEngine using the builder pattern.
 */
class NodeEngineBuilder {

  public:
    /**
     * Creates a default NodeEngineBuilder
     * @param workerConfiguration contains values that configure aspects of the NodeEngine
     * @return NodeEngineBuilder
     */
    static NodeEngineBuilder create(Configurations::WorkerConfiguration workerConfiguration);

    NodeEngineBuilder() = delete;
    /**
     * setter used to pass NesWorker to NodeEngineBuilder. Mandatory
     * @param nesWorker
     * @return NodeEngineBuilder&
     */
    NodeEngineBuilder& setNesWorker(std::weak_ptr<NesWorker>&& nesWorker);
    NodeEngineBuilder& setNodeEngineId(uint64_t nodeEngineId);
    NodeEngineBuilder& setPartitionManager(Network::PartitionManagerPtr partitionManager);
    NodeEngineBuilder& setHardwareManager(HardwareManagerPtr hardwareManager);
    NodeEngineBuilder& setBufferManagers(std::vector<BufferManagerPtr> bufferManagers);
    NodeEngineBuilder& setQueryManager(QueryManagerPtr queryManager);
    NodeEngineBuilder& setStateManager(StateManagerPtr stateManager);
    NodeEngineBuilder& setBufferStorage(BufferStoragePtr bufferStorage);
    NodeEngineBuilder& setMaterializedViewManager(Experimental::MaterializedView::MaterializedViewManagerPtr materializedViewManager);
    NodeEngineBuilder& setLanguageCompiler(std::shared_ptr<Compiler::LanguageCompiler> languageCompiler);
    NodeEngineBuilder& setJITCompiler(Compiler::JITCompilerPtr jitCompiler );
    NodeEngineBuilder& setPhaseFactory(QueryCompilation::Phases::PhaseFactoryPtr phaseFactory);
    NodeEngineBuilder& setCompiler(QueryCompilation::QueryCompilerPtr queryCompiler);


    /**
     * performs safety checks and returns a NodeEngine
     * @return NodeEnginePtr
     */
    NodeEnginePtr build();

  private:
    NodeEngineBuilder(Configurations::WorkerConfiguration workerConfiguration);
    std::weak_ptr<NesWorker> nesWorker;
    uint64_t nodeEngineId;
    Network::PartitionManagerPtr partitionManager;
    HardwareManagerPtr hardwareManager;
    std::vector<BufferManagerPtr> bufferManagers;
    QueryManagerPtr queryManager;
    StateManagerPtr stateManager;
    BufferStoragePtr bufferStorage;
    Experimental::MaterializedView::MaterializedViewManagerPtr materializedViewManager;
    std::shared_ptr<Compiler::LanguageCompiler> languageCompiler;
    Compiler::JITCompilerPtr jitCompiler;
    QueryCompilation::Phases::PhaseFactoryPtr phaseFactory;
    QueryCompilation::QueryCompilerPtr queryCompiler;
    Configurations::WorkerConfiguration workerConfiguration;

    /**
     *  Used during build() to convert the QueryCompilerConfigurations in the WorkerConfigruations to QueryCompilationOptions,
     *  which is then used to create a QueryCompiler
     * @param queryCompilerConfiguration : values to confiugre
     * @return QueryCompilerOptionsPtr
     */
    static QueryCompilation::QueryCompilerOptionsPtr
    createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration queryCompilerConfiguration);

    /**
     * @brief Returns the next free node id
     * @return node id
     */
    static uint64_t getNextNodeEngineId();
};
}// namespace NES::Runtime
#endif//NES_NODEENGINEBUILDER_HPP
