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

#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Expected.hpp>
#include <folly/Synchronized.h>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class PrintSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Print";

    explicit PrintSink(const SinkDescriptor& sinkDescriptor);
    ~PrintSink() override = default;

    PrintSink(const PrintSink&) = delete;
    PrintSink& operator=(const PrintSink&) = delete;
    PrintSink(PrintSink&&) = delete;
    PrintSink& operator=(PrintSink&&) = delete;
    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    folly::Synchronized<std::ostream*> outputStream;
    std::unique_ptr<CSVFormat> outputParser;
};

/// Todo #355 : combine configuration with source configuration (get rid of duplicated code)
struct ConfigParametersPrint
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, Configurations::InputFormat>
        INPUT_FORMAT{
            "inputFormat",
            std::nullopt,
            std::function(
                [](const std::unordered_map<std::string, std::string>& config) -> Expected<Configurations::EnumWrapper>
                { return Configurations::DescriptorConfig::tryGet(INPUT_FORMAT, config); })};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(INPUT_FORMAT);
};

}
