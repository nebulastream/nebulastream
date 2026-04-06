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

#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that broadcasts formatted TupleBuffer content as typed protobuf SystemStatEvents
/// via the SystemStatsBroadcaster. Events are silently dropped when no client is subscribed.
class GrpcStreamSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "GrpcStream";
    explicit GrpcStreamSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~GrpcStreamSink() override = default;

    GrpcStreamSink(const GrpcStreamSink&) = delete;
    GrpcStreamSink& operator=(const GrpcStreamSink&) = delete;
    GrpcStreamSink(GrpcStreamSink&&) = delete;
    GrpcStreamSink& operator=(GrpcStreamSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    std::string statType;
};

struct ConfigParametersGrpcStream
{
    static inline const DescriptorConfig::ConfigParameter<std::string> STAT_TYPE{
        "stat_type",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(STAT_TYPE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, STAT_TYPE);
};

}

FMT_OSTREAM(NES::GrpcStreamSink);
