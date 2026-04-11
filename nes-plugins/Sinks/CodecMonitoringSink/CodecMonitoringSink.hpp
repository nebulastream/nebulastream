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
#include <cstdint>
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/Format.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Strings.hpp>
#include <PipelineExecutionContext.hpp>
#include <Encoders/Encoder.hpp>

namespace NES
{

/// Measure the average, maximum, and minimum compression rate & compression ratio
class CodecMonitoringSink : public Sink
{
public:
    static constexpr std::string_view NAME = "CodecMonitoring";
    explicit CodecMonitoringSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor, std::unique_ptr<Encoder> encoder);
    ~CodecMonitoringSink() override = default;

    CodecMonitoringSink(const CodecMonitoringSink&) = delete;
    CodecMonitoringSink& operator=(const CodecMonitoringSink&) = delete;
    CodecMonitoringSink(CodecMonitoringSink&&) = delete;
    CodecMonitoringSink& operator=(CodecMonitoringSink&&) = delete;

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "CodecMonitoringSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    std::vector<double> encodingRates;
    std::vector<double> compressionRatios;
    std::vector<double> childEncodingRates;
    std::vector<double> childCompressionRatios;
    std::mutex mutex;
    std::unique_ptr<Encoder> encoder;
    size_t tupleSize;
};

struct ConfigParametersCodecMonitoring
{

    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        "NATIVE",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::CODEC, FILE_PATH, OUTPUT_FORMAT);
};

}

FMT_OSTREAM(NES::CodecMonitoringSink);