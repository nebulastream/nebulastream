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
#include <fstream>
#include <memory>
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
#include <folly/Synchronized.h>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

/// A sink that writes formatted TupleBuffers to arbitrary files.
class FileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "File";
    explicit FileSink(const SinkDescriptor& sinkDescriptor);
    ~FileSink() override = default;

    FileSink(const FileSink&) = delete;
    FileSink& operator=(const FileSink&) = delete;
    FileSink(FileSink&&) = delete;
    FileSink& operator=(FileSink&&) = delete;

    void start(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void
    execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) override;

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

}

namespace fmt
{
template <>
struct formatter<NES::Sinks::FileSink> : ostream_formatter
{
};
}
