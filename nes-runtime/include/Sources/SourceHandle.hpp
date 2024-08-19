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

#include <memory>
#include <utility>
#include <Sources/DataSource.hpp>
namespace NES
{

/**
 * @brief Todo
 */
class SourceHandle
{
public:
    explicit SourceHandle(
        OriginId originId,
        SchemaPtr schema,
        Runtime::BufferManagerPtr bufferManager,
        Runtime::QueryManagerPtr queryManager,
        size_t numSourceLocalBuffers,
        std::unique_ptr<Source> sourceImplementation,
        uint64_t numberOfBuffersToProduce,
        const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors
        = std::vector<Runtime::Execution::SuccessorExecutablePipeline>(),
        uint64_t taskQueueId = 0);

    ~SourceHandle() = default;

    bool start() const;
    bool stop(Runtime::QueryTerminationType graceful) const;

    std::string toString() const;
    OriginId getSourceId() const;

private:
    std::unique_ptr<DataSource> dataSource;
};

using SourceHandlePtr = std::shared_ptr<SourceHandle>;

} /// namespace NES
