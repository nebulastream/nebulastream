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

#include <memory>
#include <utility>
#include <Sources/DataSource.hpp>
#include <Sources/SourceHandle.hpp>

namespace NES
{
SourceHandle::SourceHandle(
    OriginId originId,
    SchemaPtr schema,
    Runtime::BufferManagerPtr bufferManager,
    Runtime::QueryManagerPtr queryManager,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation,
    uint64_t numberOfBuffersToProduce,
    const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors,
    uint64_t taskQueueId)
{
    this->dataSource = std::make_unique<DataSource>(
        originId,
        schema,
        bufferManager,
        queryManager,
        numSourceLocalBuffers,
        std::move(sourceImplementation),
        numberOfBuffersToProduce,
        executableSuccessors,
        taskQueueId);
}

bool SourceHandle::start() const
{
    return this->dataSource->start();
}
bool SourceHandle::stop(Runtime::QueryTerminationType graceful) const
{
    return this->dataSource->stop(graceful);
}

std::string SourceHandle::toString() const
{
    return this->dataSource->toString();
}

OriginId SourceHandle::getSourceId() const
{
    return this->dataSource->getOriginId();
}

} /// namespace NES