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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <folly/Synchronized.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/Format.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
/// A sink that writes formatted TupleBuffers to arbitrary files.
class FileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "File";
    explicit FileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~FileSink() override = default;

    FileSink(const FileSink&) = delete;
    FileSink& operator=(const FileSink&) = delete;
    FileSink(FileSink&&) = delete;
    FileSink& operator=(FileSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string outputFilePath;
    bool isAppend;
    bool isOpen;
    std::unique_ptr<Format> formatter;
    folly::Synchronized<std::ofstream> outputFileStream;
};

struct ConfigParametersFile
{
    static inline const DescriptorConfig::ConfigParameter<bool> APPEND{
        "append",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(APPEND, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, SinkDescriptor::FILE_PATH, APPEND);
};

}

namespace fmt
{
template <>
struct formatter<NES::FileSink> : ostream_formatter
{
};
}
