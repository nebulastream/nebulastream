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
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <SourceThread.hpp>

namespace NES
{
SourceHandle::SourceHandle(
    SourceRuntimeConfiguration configuration,
    std::unique_ptr<SourceImpl> sourceImplementation)
    : configuration(std::move(configuration)), sourceImpl(std::move(sourceImplementation))
{
}

SourceHandle::~SourceHandle() = default;

bool SourceHandle::start(SourceReturnType::EmitFunction&& emitFunction) const
{
    return this->sourceImpl->start(std::move(emitFunction));
}

void SourceHandle::stop() const
{
    this->sourceImpl->stop();
}

SourceReturnType::TryStopResult SourceHandle::tryStop(const std::chrono::milliseconds timeout) const
{
    return this->sourceImpl->tryStop(timeout);
}

OriginId SourceHandle::getSourceId() const
{
    return this->sourceImpl->getOriginId();
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return out << *sourceHandle.sourceImpl;
}

}
