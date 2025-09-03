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
#include <fstream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Formatter.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class MonitoringSink : public Sink
{
public:
    static constexpr std::string_view NAME = "Monitoring";
    explicit MonitoringSink(const SinkDescriptor& sinkDescriptor);

    /// Opens file and writes schema to file, if the file is empty.
    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&) override;
    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "MonitoringSink"; }

private:
    bool isOpen;
    std::string outputFilePath;
    std::ofstream outputFileStream;
    uint64_t sizeOfInputDataInBytes;
    std::unordered_map<SequenceNumber, uint64_t> bufferLatenciesInMS;
    uint64_t timestampOfFirstSN{};
    std::pair<SequenceNumber, uint64_t> timestampOfLastSN;
    std::mutex mutex;
};

struct ConfigParametersMonitoring
{
    static inline const DescriptorConfig::ConfigParameter<uint64_t> SIZE_OF_INPUT_DATA_IN_BYTES{
        "sizeOfInputDataInBytes",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SIZE_OF_INPUT_DATA_IN_BYTES, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::FILE_PATH, SIZE_OF_INPUT_DATA_IN_BYTES);
};

}

FMT_OSTREAM(NES::Sinks::MonitoringSink);
