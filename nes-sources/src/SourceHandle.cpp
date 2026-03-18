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
#include <Sources/TokioSource.hpp>
#include <BackpressureChannel.hpp>
#include <SourceThread.hpp>

namespace NES
{

// Overloaded helper for std::visit (same pattern used in RunningSource.cpp)
template <class... Ts>
struct Overloaded : Ts...
{
    using Ts::operator()...;
};
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

SourceHandle::SourceHandle(
    BackpressureListener backpressureListener,
    OriginId originId,
    SourceRuntimeConfiguration configuration,
    std::shared_ptr<AbstractBufferProvider> bufferPool,
    std::unique_ptr<Source> sourceImplementation)
    : configuration(std::move(configuration))
    , impl_(std::make_unique<SourceThread>(
          std::move(backpressureListener), std::move(originId), std::move(bufferPool), std::move(sourceImplementation)))
{
}

SourceHandle::SourceHandle(
    SourceRuntimeConfiguration configuration,
    std::unique_ptr<TokioSource> tokioSource)
    : configuration(std::move(configuration))
    , impl_(std::move(tokioSource))
{
}

SourceHandle::~SourceHandle() = default;

bool SourceHandle::start(SourceReturnType::EmitFunction&& emitFunction) const
{
    return std::visit(
        Overloaded{
            [&](const std::unique_ptr<SourceThread>& st)
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
                return st->start(std::move(wrappedEmit));
            },
            [&](const std::unique_ptr<TokioSource>& ts)
            {
                // TokioSource handles inflight limiting on the Rust side
                // via Arc<Semaphore> + onComplete callback in bridge_emit.
                return ts->start(std::move(emitFunction));
            }
        },
        impl_);
}

void SourceHandle::stop() const
{
    std::visit([](auto& impl) { impl->stop(); }, impl_);
}

SourceReturnType::TryStopResult SourceHandle::tryStop(const std::chrono::milliseconds timeout) const
{
    return std::visit([&](auto& impl) { return impl->tryStop(timeout); }, impl_);
}

OriginId SourceHandle::getSourceId() const
{
    return std::visit(
        Overloaded{
            [](const std::unique_ptr<SourceThread>& st) { return st->getOriginId(); },
            [](const std::unique_ptr<TokioSource>& ts) { return ts->getSourceId(); }
        },
        impl_);
}

std::ostream& operator<<(std::ostream& out, const SourceHandle& sourceHandle)
{
    return std::visit(
        Overloaded{
            [&](const std::unique_ptr<SourceThread>& st) -> std::ostream& { return out << *st; },
            [&](const std::unique_ptr<TokioSource>& ts) -> std::ostream& { return out << *ts; }
        },
        sourceHandle.impl_);
}

}
