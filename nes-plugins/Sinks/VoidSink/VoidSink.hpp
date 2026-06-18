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

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/SchemaFormatter.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that simply dumps input tuples into the void
/// As such the output written to file will always be
/// an empty line
class VoidSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "Void";
    explicit VoidSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "VoidSink"; }

private:
    /// Optional result-file path: present when a harness (e.g. systest) injects one, absent in a
    /// normal query. When present, only the schema header line (no data rows) is written to it on
    /// stop, so the systest result check sees a matching schema and an empty result set.
    std::optional<std::string> outputFilePath;
    SchemaFormatter schemaFormatter;
};

struct ConfigParametersVoid
{
    /// The Void sink discards all tuples and has no real configuration. It nonetheless accepts an
    /// optional `file_path` because the systest harness injects one into every sink; it is used only
    /// to emit a single empty line so the harness has a result file to read.
    /// NOLINTNEXTLINE(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH);
};
}

FMT_OSTREAM(NES::VoidSink);
