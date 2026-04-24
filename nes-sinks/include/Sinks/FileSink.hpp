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
#include <Sinks/FileSinkConfig.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
/// A sink that writes formatted TupleBuffers to arbitrary files.
class FileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = FileSinkName::NAME;
    explicit FileSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~FileSink() override = default;

    FileSink(const FileSink&) = delete;
    FileSink& operator=(const FileSink&) = delete;
    FileSink(FileSink&&) = delete;
    FileSink& operator=(FileSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string outputFilePath;
    bool isAppend;
    bool isOpen;
    folly::Synchronized<std::ofstream> outputFileStream;
    SchemaFormatter schemaFormatter;
};

}

namespace fmt
{
template <>
struct formatter<NES::FileSink> : ostream_formatter
{
};
}
