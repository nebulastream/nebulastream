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
namespace NES::Runtime {
enum class NumaAwarenessFlag { ENABLED, DISABLED };
class NodeEngineBuilder {

  public:
    static NodeEngineBuilder create();

    NodeEngineBuilder& setHostname(const std::string& hostname);
    NodeEngineBuilder& setPort(uint16_t port);
    NodeEngineBuilder& setPhysicalSources(const std::vector<PhysicalSourcePtr>& physicalSources);
    NodeEngineBuilder& setNumThreads(uint16_t numThreads);
    NodeEngineBuilder& setBufferSize(uint64_t bufferSize);
    NodeEngineBuilder& setNumberOfBuffersInGlobalBufferManager(uint64_t numberOfBuffersInGlobalBufferManager);
    NodeEngineBuilder& setNumberOfBuffersInSourceLocalBufferPool(uint64_t numberOfBuffersInSourceLocalBufferPool);
    NodeEngineBuilder& setNumberOfBuffersPerWorker(uint64_t numberOfBuffersPerWorker);
    NodeEngineBuilder& setQueryCompilerConfiguration(const Configurations::QueryCompilerConfiguration& queryCompilerConfiguration);
    NodeEngineBuilder& setNesWorker(const std::weak_ptr<NesWorker>&& nesWorker);
    NodeEngineBuilder& setEnableNumaAwareness(NumaAwarenessFlag enableNumaAwareness);
    NodeEngineBuilder& setWorkerToCoreMapping(const std::string& workerToCoreMapping);
    NodeEngineBuilder& setNumberOfQueues(uint64_t numberOfQueues);
    NodeEngineBuilder& setNumberOfThreadsPerQueue(uint64_t numberOfThreadsPerQueue);
    NodeEngineBuilder& setQueryManagerMode(QueryManager::QueryMangerMode queryManagerMode);
    NodeEnginePtr build();

  private:
    static QueryCompilation::QueryCompilerOptionsPtr
    createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration queryCompilerConfiguration);

    /**
     * @brief Returns the next free node id
     * @return node id
     */
    static uint64_t getNextNodeEngineId();

    std::string hostname;
    uint16_t port;
    bool isPortSet;
    std::vector<PhysicalSourcePtr> physicalSources;
    uint16_t numThreads;
    uint64_t bufferSize;
    uint64_t numberOfBuffersInGlobalBufferManager;
    uint64_t numberOfBuffersInSourceLocalBufferPool;
    uint64_t numberOfBuffersPerWorker;
    Configurations::QueryCompilerConfiguration queryCompilerConfiguration;
    bool isQueryCompilerConfigurationSet;
    std::weak_ptr<NesWorker> nesWorker;
    NumaAwarenessFlag enableNumaAwareness;
    bool isNumaAwarenessFlagSet;
    std::string workerToCoreMapping;
    uint64_t numberOfQueues;
    uint64_t numberOfThreadsPerQueue;
    QueryManager::QueryMangerMode queryManagerMode;
    bool isQueryManagerModeSet;
};
}// namespace NES::Runtime
#endif//NES_NODEENGINEBUILDER_HPP
