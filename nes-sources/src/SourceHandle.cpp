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
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <AsyncSourceRunner.hpp>

namespace NES::Sources
{
SourceHandle::SourceHandle(
    OriginId originId,
    std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
    Sources::SourceReturnType::EmitFunction&& emitFunction,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImpl,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter,
    std::shared_ptr<AsyncSourceExecutor> executor)
{
    this->sourceRunner = std::make_unique<AsyncSourceRunner>(
        std::move(originId),
        std::move(bufferPool),
        std::move(emitFunction),
        numSourceLocalBuffers,
        std::move(sourceImpl),
        std::move(inputFormatter),
        executor);
}
SourceHandle::~SourceHandle() = default;

void SourceHandle::start() const
{
    return this->sourceRunner->start();
}
void SourceHandle::stop() const
{
    return this->sourceRunner->stop();
}

OriginId SourceHandle::getSourceId() const
{
    return this->sourceRunner->getOriginId();
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return out << *sourceHandle.sourceRunner;
}

}
