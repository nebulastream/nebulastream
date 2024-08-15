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

#pragma once

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

    explicit NodeEngineBuilder(const Configurations::WorkerConfiguration& workerConfiguration);

    NodeEngineBuilder& setBufferManagers(std::vector<Memory::BufferManagerPtr> bufferManagers);
    NodeEngineBuilder& setQueryManager(QueryManagerPtr queryManager);

    std::unique_ptr<NodeEngine> build();

private:
    std::vector<Memory::BufferManagerPtr> bufferManagers;
    QueryManagerPtr queryManager;
    const Configurations::WorkerConfiguration& workerConfiguration;
};
} /// namespace NES::Runtime
