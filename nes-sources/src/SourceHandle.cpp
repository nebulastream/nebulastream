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

#include <Sources/SourceHandle.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <BackpressureChannel.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <SourceThread.hpp>

namespace NES::Sources
{
SourceHandle::SourceHandle(
    Ingestion ingestion,
    OriginId originId,
    std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
    size_t numberOfBuffersInLocalPool,
    std::unique_ptr<Source> sourceImplementation)
{
    this->sourceThread = std::make_unique<SourceThread>(
        std::move(ingestion), std::move(originId), std::move(bufferPool), numberOfBuffersInLocalPool, std::move(sourceImplementation));
}
SourceHandle::~SourceHandle() = default;

bool SourceHandle::start(SourceReturnType::EmitFunction&& emitFunction) const
{
    return this->sourceThread->start(std::move(emitFunction));
}
void SourceHandle::stop() const
{
    this->sourceThread->stop();
}
SourceReturnType::TryStopResult SourceHandle::tryStop(const std::chrono::milliseconds timeout) const
{
    return this->sourceThread->tryStop(timeout);
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
