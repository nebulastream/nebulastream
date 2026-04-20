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

#include <SourceThreadHandle.hpp>

#include <cstddef>
#include <limits>
#include <memory>
#include <ostream>
#include <semaphore>
#include <stop_token>
#include <utility>
#include <variant>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceReturnType.hpp>
#include <BackpressureChannel.hpp>
#include <SourceThread.hpp>

#include "QueryId.hpp"

namespace NES
{

SourceThreadHandle::SourceThreadHandle(
    BackpressureListener backpressureListener,
    OriginId originId,
    SourceRuntimeConfiguration configuration,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    std::unique_ptr<Source> sourceImplementation)
    : configuration(std::move(configuration))
    , impl(std::make_unique<SourceThread>(
          std::move(backpressureListener), std::move(originId), std::move(bufferPool), std::move(sourceImplementation)))
{
}

SourceThreadHandle::~SourceThreadHandle() = default;

bool SourceThreadHandle::start(QueryId queryId, SourceReturnType::EmitFunction&& emitFunction, SourceReturnType::AsyncEmitFunction&&)
{
    // Wrap the emitFunction with a semaphore for inflight limiting.
    // The semaphore is acquired before each data emit (blocking the source thread)
    // and released in the onComplete callback after the pipeline processes the buffer.
    auto sem = std::make_shared<std::counting_semaphore<>>(
        std::min(configuration.inflightBufferLimit, static_cast<size_t>(std::numeric_limits<int32_t>::max())));
    auto wrappedEmit = [sem, emitFn = std::move(emitFunction)](
                           const OriginId sourceId,
                           SourceReturnType::SourceReturnType event,
                           const std::stop_token& stopToken) -> SourceReturnType::EmitResult
    {
        if (auto* data = std::get_if<SourceReturnType::Data>(&event))
        {
            {
                const std::stop_callback callback(stopToken, [&]() { sem->release(); });
                sem->acquire();
                if (stopToken.stop_requested())
                {
                    return SourceReturnType::EmitResult::STOP_REQUESTED;
                }
            }
            data->onComplete = [sem] { sem->release(); };
        }
        return emitFn(sourceId, std::move(event), stopToken);
    };
    return impl->start(queryId, std::move(wrappedEmit));
}

void SourceThreadHandle::stop()
{
    impl->stop();
}

SourceReturnType::TryStopResult SourceThreadHandle::tryStop(const std::chrono::milliseconds timeout)
{
    return impl->tryStop(timeout);
}

OriginId SourceThreadHandle::getSourceId() const
{
    return impl->getOriginId();
}

std::ostream& SourceThreadHandle::toString(std::ostream& os) const
{
    return os << *impl;
}

}
