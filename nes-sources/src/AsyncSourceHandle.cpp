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

#include <AsyncSourceHandle.hpp>

#include <ostream>
#include <utility>
#include <TokioSource.hpp>

namespace NES
{

AsyncSourceHandle::AsyncSourceHandle(
    BackpressureListener listener, SourceDescriptor descriptor, OriginId originId, std::shared_ptr<AbstractBufferProvider> bufferProvider)
    : configuration{0}
    , originId(originId)
    , bufferProvider(std::move(bufferProvider))
    , impl(std::make_unique<TokioSource>(std::move(listener), std::move(descriptor), std::move(originId)))
{
}

AsyncSourceHandle::~AsyncSourceHandle() = default;

bool AsyncSourceHandle::start(SourceReturnType::EmitFunction&&, SourceReturnType::AsyncEmitFunction&& asyncEmitFunction)
{
    return impl->start(std::move(asyncEmitFunction), bufferProvider);
}

void AsyncSourceHandle::stop()
{
    impl->stop();
}

SourceReturnType::TryStopResult AsyncSourceHandle::tryStop(const std::chrono::milliseconds)
{
    /// TokioSource does not currently support graceful shutdown with timeout
    impl->stop();
    return SourceReturnType::TryStopResult::SUCCESS;
}

OriginId AsyncSourceHandle::getSourceId() const
{
    return originId;
}

std::ostream& AsyncSourceHandle::toString(std::ostream& os) const
{
    return os << *impl;
}

}
