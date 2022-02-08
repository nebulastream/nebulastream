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

#ifndef NES_INCLUDE_RUNTIME_NODEENGINEFACTORY_HPP_
#define NES_INCLUDE_RUNTIME_NODEENGINEFACTORY_HPP_
#include <Runtime/RuntimeForwardRefs.hpp>

#include <vector>

namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace Configurations{
class QueryCompilerConfiguration;
}

namespace Runtime {
enum class NumaAwarenessFlag { ENABLED, DISABLED };
/**
 * @brief A general factory to create a node engine given some configuration.
 */
class NodeEngineFactory {
  public:
    /**
     * @brief this creates a new Runtime with some default parameters
     * @param hostname the ip address for the network manager
     * @param port the port for the network manager
     * @param config
     * @return
     */
    static NodeEnginePtr createDefaultNodeEngine(const std::string& hostname, uint16_t port, std::vector<PhysicalSourcePtr> physicalSources);

    /**
    * @brief this creates a new Runtime
    * @param hostname the ip address for the network manager
    * @param port the port for the network manager
    * @param numThreads the number of worker threads for this nodeEngine
    * @param bufferSize the buffer size for the buffer manager
    * @param numBuffers the number of buffers for the buffer manager
    * @return
    */
    static NodeEnginePtr createNodeEngine(const std::string& hostname,
                                          uint16_t port,
                                          std::vector<PhysicalSourcePtr> physicalSources,
                                          uint16_t numThreads,
                                          uint64_t bufferSize,
                                          uint64_t numberOfBuffersInGlobalBufferManager,
                                          uint64_t numberOfBuffersInSourceLocalBufferPool,
                                          uint64_t numberOfBuffersPerWorker,
                                          const Configurations::QueryCompilerConfiguration queryCompilerConfiguration,
                                          NumaAwarenessFlag enableNumaAwareness = NumaAwarenessFlag::DISABLED,
                                          const std::string& workerToCodeMapping = "",
                                          const std::string& queuePinList = "");

  private:
    static QueryCompilation::QueryCompilerOptionsPtr
    createQueryCompilationOptions(const Configurations::QueryCompilerConfiguration queryCompilerConfiguration);

    /**
     * @brief Returns the next free node id
     * @return node id
     */
    static uint64_t getNextNodeEngineId();
};

}// namespace Runtime
}// namespace NES
#endif  // NES_INCLUDE_RUNTIME_NODEENGINEFACTORY_HPP_
