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

#ifndef NES_EXECUTION_INCLUDE_RUNTIME_NODEENGINEBUILDER_HPP_
#define NES_EXECUTION_INCLUDE_RUNTIME_NODEENGINEBUILDER_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::Runtime
{
/**
 * This class is used to create instances of NodeEngine using the builder pattern.
 */
class NodeEngineBuilder
{
public:
    NodeEngineBuilder() = delete;
    /**
     * Creates a default NodeEngineBuilder
     * @param workerConfiguration contains values that configure aspects of the NodeEngine
     * @return NodeEngineBuilder
     */
    explicit NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration);

    /**
     * setter used to pass a vector of buffer managers to NodeEngineBuilder. Optional
     * @param bufferManagers list of BufferProvider
     * @return NodeEngineBuilder&
     */
    NodeEngineBuilder& setBufferManagers(std::vector<BufferManagerPtr> bufferManagers);

    /**
     * setter used to pass a query manager to NodeEngineBuilder. Optional
     * @param queryManager
     * @return NodeEngineBuilder&
     */
    NodeEngineBuilder& setQueryManager(QueryManagerPtr queryManager);

    /**
     * performs safety checks and returns a NodeEngine
     * @return NodeEnginePtr
     */
    std::unique_ptr<NodeEngine> build();

private:
    std::vector<BufferManagerPtr> bufferManagers;
    QueryManagerPtr queryManager;
    const Configurations::WorkerConfiguration& workerConfiguration;
};
} /// namespace NES::Runtime
#endif /// NES_EXECUTION_INCLUDE_RUNTIME_NODEENGINEBUILDER_HPP_
