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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceReturnType.hpp>
#include <BackpressureChannel.hpp>
#include "Sources/SourceHandle.hpp"

#include "QueryId.hpp"

namespace NES
{

struct TokioSourceContext;

class TokioSource : public SourceHandle
{
public:
    TokioSource(
        BackpressureListener listener,
        SourceDescriptor descriptor,
        OriginId originId,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        SourceRuntimeConfiguration runtimeConfiguration);
    ~TokioSource() override;

    // Non-copyable, non-movable
    TokioSource(const TokioSource&) = delete;
    TokioSource& operator=(const TokioSource&) = delete;
    TokioSource(TokioSource&&) = delete;
    TokioSource& operator=(TokioSource&&) = delete;

    friend std::ostream& operator<<(std::ostream& out, const TokioSource& source);
    bool start(QueryId queryId, SourceReturnType::EmitFunction&& emitFunction, SourceReturnType::AsyncEmitFunction&& asyncEmitFunction) override;
    void stop() override;
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) override;
    [[nodiscard]] OriginId getSourceId() const override;
    [[nodiscard]] const SourceRuntimeConfiguration& getRuntimeConfiguration() const override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    std::unique_ptr<TokioSourceContext> context;
    OriginId originId;
    SourceDescriptor descriptor;
    BackpressureListener backpressureHandler;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;
    SourceRuntimeConfiguration runtimeConfiguration;
};

} // namespace NES
