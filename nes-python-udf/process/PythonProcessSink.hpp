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
#include <expected>
#include <ostream>
#include <string>
#include <string_view>
#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <PythonProcessChannel.hpp>

namespace NES
{

struct PythonProcessSinkConfig
{
    /// Id of the channel the `PythonProcess` source of the same UDF operator takes the buffers from.
    std::string channel;

    static std::expected<PythonProcessSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// Ends the pipeline in front of an out-of-process UDF operator. Hands its Arrow-layout buffers to the source of the operator,
/// which exchanges them with the process. If the process falls behind, this holds back the query's sources.
class PythonProcessSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "PythonProcess";
    static constexpr auto RETRY_INTERVAL = std::chrono::milliseconds(10);

    PythonProcessSink(BackpressureController backpressureController, const PythonProcessSinkConfig& config);

    void start(PipelineExecutionContext&) override { }

    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "PythonProcessSink"; }

private:
    /// Once the source has stopped: drops what is still to be handed over and releases the query's sources.
    void dropRemaining(const TupleBuffer& buffer);

    PythonProcessChannel::Lease channel;
    /// Several worker threads may run `execute` at once.
    std::atomic<bool> consumerGone = false;
    BackpressureHandler backpressureHandler;
};

}

FMT_OSTREAM(NES::PythonProcessSink);
