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
#include <SourceThread.hpp>

namespace NES::Sources
{
SourceHandle::SourceHandle(
    OriginId originId,
    std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
    Sources::SourceReturnType::EmitFunction&& emitFunction,
    size_t numSourceLocalBuffers,
    std::unique_ptr<Source> sourceImplementation,
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter)
{
    this->sourceThread = std::make_unique<SourceThread>(
        std::move(originId),
        std::move(bufferPool),
        std::move(emitFunction),
        numSourceLocalBuffers,
        std::move(sourceImplementation),
        std::move(inputFormatter));
}
SourceHandle::~SourceHandle() = default;

bool SourceHandle::start() const
{
    return this->sourceThread->start();
}
bool SourceHandle::stop() const
{
    return this->sourceThread->stop();
}

OriginId SourceHandle::getSourceId() const
{
    return this->sourceThread->getOriginId();
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return out << *sourceHandle.sourceThread;
}

}
