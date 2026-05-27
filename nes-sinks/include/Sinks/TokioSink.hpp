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
#include <Sinks/Sink.hpp>
#include <folly/Synchronized.h>
#include <BackpressureChannel.hpp>

#include "SinkDescriptor.hpp"

namespace NES
{

struct TokioSinkContext;
class SinkBackpressureHandler;

class TokioSink final : public Sink
{
public:

    TokioSink(
        BackpressureController backpressureController,
        SinkDescriptor descriptor,
        size_t upperThreshold = 20,
        size_t lowerThreshold = 10);
    ~TokioSink() override;

    // Non-copyable, non-movable
    TokioSink(const TokioSink&) = delete;
    TokioSink& operator=(const TokioSink&) = delete;
    TokioSink(TokioSink&&) = delete;
    TokioSink& operator=(TokioSink&&) = delete;

    void start(PipelineExecutionContext& pec) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext& pec) override;
    void stop(PipelineExecutionContext& pec) override;

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    SinkDescriptor descriptor;
    std::unique_ptr<TokioSinkContext> context;
    std::unique_ptr<SinkBackpressureHandler> backpressureHandler;
    std::atomic<bool> stopInitiated{false};
    uint64_t sinkId{0}; /// Assigned in start() from pipeline context
};

} // namespace NES
