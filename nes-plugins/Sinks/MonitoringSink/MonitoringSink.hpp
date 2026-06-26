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

namespace NES
{

class MonitoringSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Monitoring";
    explicit MonitoringSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~MonitoringSink() override = default;

    MonitoringSink(const MonitoringSink&) = delete;
    MonitoringSink& operator=(const MonitoringSink&) = delete;
    MonitoringSink(MonitoringSink&&) = delete;
    MonitoringSink& operator=(MonitoringSink&&) = delete;

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "MonitoringSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    uint64_t sizeOfInputDataInBytes;
    /// Latencies/timestamps are in MICROSECONDS: the source stamps sourceCreationTimestamp at read-start
    /// in us (AsyncSourceRunner) and this sink records arrival in us, so latency = arrival - read-start =
    /// true end-to-end latency including the read/recv copy (ms resolution was too coarse: 0/1 ms).
    std::unordered_map<SequenceNumber, uint64_t> bufferLatenciesInUS;
    uint64_t timestampOfFirstSN{};
    std::pair<SequenceNumber, uint64_t> timestampOfLastSN;
    std::mutex mutex;
    std::unique_ptr<Format> format;
};

struct ConfigParametersMonitoring
{
    static inline const DescriptorConfig::ConfigParameter<uint64_t> SIZE_OF_INPUT_DATA_IN_BYTES{
        "size_of_input_data_in_bytes",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SIZE_OF_INPUT_DATA_IN_BYTES, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> OUTPUT_FORMAT{
        "output_format",
        "NATIVE",
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(OUTPUT_FORMAT, config); }};


    /// Addition to seamlessly test the old output formatter. The old output formatter can only be activated, if OUTPUT_FORMAT is Native.
    /// Otherwise, legacy_output_format will switch to None.
    static inline const DescriptorConfig::ConfigParameter<std::string> LEGACY_OUTPUT_FORMAT{
        "legacy_output_format",
        "None",
        [](const std::unordered_map<std::string, std::string>& config)
        {
            return (!DescriptorConfig::tryGet(OUTPUT_FORMAT, config).has_value()
                    || toUpperCase(DescriptorConfig::tryGet(OUTPUT_FORMAT, config).value()) == "NATIVE")
                ? DescriptorConfig::tryGet(LEGACY_OUTPUT_FORMAT, config)
                : std::optional("None");
        }};

    static inline const DescriptorConfig::ConfigParameter<std::string> FILE_PATH{
        "file_path",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FILE_PATH, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(FILE_PATH, SIZE_OF_INPUT_DATA_IN_BYTES, OUTPUT_FORMAT, LEGACY_OUTPUT_FORMAT);
};

}

FMT_OSTREAM(NES::MonitoringSink);
