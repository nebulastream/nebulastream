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
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
/// Discards tuple buffers
class DiscardSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Discard";
    explicit DiscardSink(const SinkDescriptor& sinkDescriptor);
    ~DiscardSink() override = default;

    DiscardSink(const DiscardSink&) = delete;
    DiscardSink& operator=(const DiscardSink&) = delete;
    DiscardSink(DiscardSink&&) = delete;
    DiscardSink& operator=(DiscardSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;
};

struct ConfigParametersDiscard
{
    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
        "input_format",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, SinkDescriptor::FILE_PATH, INPUT_FORMAT);
};

}
