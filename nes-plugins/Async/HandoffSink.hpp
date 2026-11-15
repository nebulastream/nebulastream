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
#include <ostream>
#include <string_view>
#include <Async/HandoffChannel.hpp>
#include <Configurations/ConfigField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/HandoffSinkConfig.hpp>
#include <Sinks/Sink.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Producer half of an asynchronously executed operator. Both endpoint configurations
/// hold the same channel; full queues apply backpressure to the producer's sources.
class HandoffSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Handoff";
    static constexpr auto RETRY_INTERVAL = std::chrono::milliseconds(10);
    explicit HandoffSink(BackpressureController backpressureController, const HandoffSinkConfig& config);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "HandoffSink"; }

private:
    /// Once the Async source has stopped: drops pending buffers and releases the producer's sources.
    void dropRemaining(const TupleBuffer& buffer);

    HandoffChannel::Sender channel;
    /// Several worker threads may run execute at once.
    std::atomic<bool> consumerGone = false;
    BackpressureHandler backpressureHandler;
};

}

FMT_OSTREAM(NES::HandoffSink);
