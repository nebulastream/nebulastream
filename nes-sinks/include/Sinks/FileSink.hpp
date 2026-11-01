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

#include <expected>
#include <filesystem>
#include <fstream>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>

#include <folly/Synchronized.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Sink-defined config struct: instantiated from the generic config by the SinkConfig registry
/// entry, carried through the SinkDescriptor as std::any, and serialized via reflection of
/// exactly this struct (all members are reflectable).
struct FileSinkConfig
{
    std::filesystem::path filePath;
    bool append;

    static std::expected<FileSinkConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

/// A sink that writes formatted TupleBuffers to files or existing Unix-domain sockets.
class FileSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "File";
    explicit FileSink(BackpressureController backpressureController, const FileSinkConfig& config, const SinkDescriptor& sinkDescriptor);
    ~FileSink() override = default;

    FileSink(const FileSink&) = delete;
    FileSink& operator=(const FileSink&) = delete;
    FileSink(FileSink&&) = delete;
    FileSink& operator=(FileSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

protected:
    std::ostream& toString(std::ostream& str) const override;


private:
    std::string outputFilePath;
    bool isAppend;

    struct SocketOutput
    {
        explicit SocketOutput(const std::string& path);
        ~SocketOutput();

        SocketOutput(const SocketOutput&) = delete;
        SocketOutput& operator=(const SocketOutput&) = delete;
        SocketOutput(SocketOutput&&) = delete;
        SocketOutput& operator=(SocketOutput&&) = delete;

        int descriptor = -1;
    };

    /// Exactly one output is active; monostate represents an unopened/stopped sink.
    using Output = std::variant<std::monostate, std::ofstream, SocketOutput>;
    void write(Output& destination, std::string_view contents);

    folly::Synchronized<Output> output;
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
