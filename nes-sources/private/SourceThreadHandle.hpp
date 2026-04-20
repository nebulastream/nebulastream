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

#include <memory>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>

#include "QueryId.hpp"

namespace NES
{

class SourceThread;

/// SourceHandle implementation for C++ sources that run on a dedicated SourceThread.
class SourceThreadHandle final : public SourceHandle
{
public:
    explicit SourceThreadHandle(
        BackpressureListener backpressureListener,
        OriginId originId,
        SourceRuntimeConfiguration configuration,
        std::shared_ptr<AbstractBufferProvider> bufferPool,
        std::unique_ptr<Source> sourceImplementation);

    ~SourceThreadHandle() override;

    bool start(QueryId queryId, SourceReturnType::EmitFunction&& emitFunction, SourceReturnType::AsyncEmitFunction&& asyncEmitFunction) override;
    void stop() override;
    [[nodiscard]] SourceReturnType::TryStopResult tryStop(std::chrono::milliseconds timeout) override;
    [[nodiscard]] OriginId getSourceId() const override;
    [[nodiscard]] const SourceRuntimeConfiguration& getRuntimeConfiguration() const override { return configuration; }

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    SourceRuntimeConfiguration configuration;
    std::unique_ptr<SourceThread> impl;
};

}
