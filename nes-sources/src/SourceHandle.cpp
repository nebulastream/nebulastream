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
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <DataSource.hpp>
#include <SourceHandle.hpp>

namespace NES
{
SourceHandle::SourceHandle(
    OriginId originId,
    SchemaPtr schema,
    std::shared_ptr<Runtime::AbstractPoolProvider> bufferPool,
    SourceReturnType::EmitFunction&& emitFunction,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation,
    uint64_t numberOfBuffersToProduce)
{
    this->dataSource = std::make_unique<DataSource>(
        originId,
        schema,
        std::move(bufferPool),
        std::move(emitFunction),
        numSourceLocalBuffers,
        std::move(sourceImplementation),
        numberOfBuffersToProduce);
}

bool SourceHandle::start() const
{
    return this->dataSource->start();
}
bool SourceHandle::stop() const
{
    return this->dataSource->stop();
}

const DataSource* SourceHandle::getDataSource() const
{
    return this->dataSource.get();
}

OriginId SourceHandle::getSourceId() const
{
    return this->dataSource->getOriginId();
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return out << sourceHandle.getDataSource();
}
} /// namespace NES