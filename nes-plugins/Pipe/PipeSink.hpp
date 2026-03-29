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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipeService.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that writes TupleBuffers into a named pipe, distributing them to all registered PipeSource instances.
/// New sources are activated at sequence boundaries (when a new sequence number exceeds the max completed one)
/// to avoid delivering partial chunked data.
/// Backpressure is applied when no consumers are connected and released when the first consumer registers.
class PipeSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Pipe";

    explicit PipeSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~PipeSink() override;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "PipeSink(pipe_name=" << pipeName << ")"; }

private:
    std::string pipeName;
    std::shared_ptr<const Schema> schema;
    std::shared_ptr<PipeService::SinkHandle> sinkHandle;
    /// Highest sequence number seen in any delivered buffer.
    /// New sources are only activated when a truly new sequence arrives (seqNum > maxSeqStarted),
    /// preventing mid-sequence promotion that would cause partial data.
    SequenceNumber maxSeqStarted = INVALID_SEQ_NUMBER;
};

struct ConfigParametersPipeSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> PIPE_NAME{
        "pipe_name",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PIPE_NAME, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(PIPE_NAME);
};

}

FMT_OSTREAM(NES::PipeSink);
