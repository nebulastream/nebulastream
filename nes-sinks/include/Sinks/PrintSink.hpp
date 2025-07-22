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

#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <folly/Synchronized.h>

#include <BackpressureChannel.hpp>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class PrintSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Print";

    explicit PrintSink(Valve valve, const SinkDescriptor& sinkDescriptor);
    ~PrintSink() override = default;

    PrintSink(const PrintSink&) = delete;
    PrintSink& operator=(const PrintSink&) = delete;
    PrintSink(PrintSink&&) = delete;
    PrintSink& operator=(PrintSink&&) = delete;
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    folly::Synchronized<std::ostream*> outputStream;
    std::unique_ptr<CSVFormat> outputParser;

    uint32_t ingestion = 0;
};

/// Todo #355 : combine configuration with source configuration (get rid of duplicated code)
struct ConfigParametersPrint
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> INGESTION{
        "ingestion",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(INGESTION, config); }};

    static inline const DescriptorConfig::ConfigParameter<EnumWrapper, InputFormat> INPUT_FORMAT{
        "inputFormat",
        std::nullopt,
        std::function(
                [](const std::unordered_map<std::string, std::string>& config) -> Expected<Configurations::EnumWrapper>
                { return DescriptorConfig::tryGet(INPUT_FORMAT, config); })};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(INGESTION, INPUT_FORMAT);
};

}
