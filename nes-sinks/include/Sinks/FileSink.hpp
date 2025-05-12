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

#include <fstream>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <optional>

#include <folly/Synchronized.h>

#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

/// A sink that writes formatted TupleBuffers to arbitrary files.
class FileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "File";
    explicit FileSink(Valve valve, const SinkDescriptor& sinkDescriptor);
    ~FileSink() override = default;

    FileSink(const FileSink&) = delete;
    FileSink& operator=(const FileSink&) = delete;
    FileSink(FileSink&&) = delete;
    FileSink& operator=(FileSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string outputFilePath;
    bool isAppend;
    bool isOpen;
    /// Todo #417: support abstract/arbitrary formatter
    std::unique_ptr<CSVFormat> formatter;
    folly::Synchronized<std::ofstream> outputFileStream;
};

/// Todo #355 : combine configuration with source configuration (get rid of duplicated code)
struct ConfigParametersFile
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, Configurations::InputFormat>
        INPUT_FORMAT{
            "inputFormat",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return Configurations::DescriptorConfig::tryGet(INPUT_FORMAT, config); }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(FILEPATH, config); }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<bool> APPEND{
        "append",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(APPEND, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(INPUT_FORMAT, FILEPATH, APPEND);
};

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::FileSink> : ostream_formatter
{
};
}
