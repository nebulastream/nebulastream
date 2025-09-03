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

#include <MonitoringSink.hpp>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include "Identifiers/NESStrongType.hpp"
#include "Util/Logger/Formatter.hpp"

namespace NES
{
struct LatencyMeasurements
{
    uint64_t minLatency = 0;
    uint64_t maxLatency = std::numeric_limits<uint64_t>::max();
    double avgLatency = 0;

    // friend std::ostream& operator<<(std::ostream& os, const SequenceData& obj);
    friend std::ostream& operator<<(std::ostream& os, const LatencyMeasurements& latencyMeasurements)
    {
        return os << fmt::format(
                   "average latency, min latency, max latency\n{},{},{}",
                   latencyMeasurements.avgLatency,
                   latencyMeasurements.minLatency,
                   latencyMeasurements.maxLatency);
    }
};
}

FMT_OSTREAM(NES::LatencyMeasurements);

namespace NES::Sinks
{

MonitoringSink::MonitoringSink(const SinkDescriptor& sinkDescriptor)
    : isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    // Todo: needs to be fileSize * repetitions * numClients
    , sizeOfInputDataInBytes(sinkDescriptor.getFromConfig(ConfigParametersMonitoring::SIZE_OF_INPUT_DATA_IN_BYTES))
    // , sizeOfInputDataInBytes(497214231)
    , timestampOfLastSN(std::make_pair(INVALID<SequenceNumber>, 0))
{
}

void MonitoringSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up monitoring sink: {}", *this);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath.c_str(), ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
        }
    }

    /// Open the file stream
    if (!outputFileStream.is_open())
    {
        outputFileStream.open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFileStream.is_open() && outputFileStream.good();
    if (!isOpen)
    {
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}",
            outputFilePath,
            outputFileStream.is_open(),
            outputFileStream.good());
    }
}

// Todo: how to measure latency?
// in theory:
// - collect creation TS in MS for every buffer
// - collect sink arrival TS in MS for every buffer
//  -> calculate latencies for buffers by measuring difference of (arrival - creation) <- how long did it take for a buffer to pass through the system
//      -> a sequence number may be split over multiple chunk-buffers at sink, simply measure 'isLastChunk'-buffer? even though it may not be last chunk?
//  -> calculate throughput by measuring ('creation of first buffer' - 'arrival of last buffer')
void MonitoringSink::stop(PipelineExecutionContext&)
{
    NES_WARNING("Stopping monitoring sink");
    if (this->bufferLatenciesInMS.empty())
    {
        NES_WARNING("Latencies empty");
        return;
    }

    const auto latencyMeasurements = [](const std::unordered_map<SequenceNumber, uint64_t>& bufferLatencies)
    {
        uint64_t minLatency = std::numeric_limits<uint64_t>::max();
        uint64_t maxLatency = 0;
        uint64_t latencySum = 0;
        for (const auto latency : bufferLatencies | std::views::values)
        {
            minLatency = (latency != 0) ? std::min(latency, minLatency) : minLatency;
            maxLatency = std::max(latency, minLatency);
            latencySum += latency;
        }
        return LatencyMeasurements{
            .minLatency = minLatency, .maxLatency = maxLatency, .avgLatency = latencySum / static_cast<double>(bufferLatencies.size())};
    }(this->bufferLatenciesInMS);

    // Todo: is wrong for repititions
    const auto processingTimeInSeconds = (this->timestampOfLastSN.second - this->timestampOfFirstSN) / static_cast<double>(1000);
    const auto inputSizeInMegaBytes = this->sizeOfInputDataInBytes / 1000000;
    const auto throughputInMegaBytesPerSecond = inputSizeInMegaBytes / processingTimeInSeconds;
    // outputFileStream << fmt::format("Throughput in MB/s {}, {}", throughputInMegaBytesPerSecond, latencyMeasurements);
    outputFileStream << fmt::format(
        "throughput in MB/s,average latency,min latency,max latency\n{},{},{},{}\n",
        throughputInMegaBytesPerSecond,
        latencyMeasurements.avgLatency,
        latencyMeasurements.minLatency,
        latencyMeasurements.maxLatency);
    outputFileStream.close();
    isOpen = false;
}

void MonitoringSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in MonitoringSink.");

    // if (inputBuffer.isLastChunk())
    /// If the current buffer contains the source creation timestamp marker, measure the latency
    if (inputBuffer.getSourceCreationTimestampInMS().getRawValue() != Timestamp::INITIAL_VALUE)
    {
        const std::scoped_lock lock(mutex);
        const auto arrivalTimeInMS = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
        const auto bufferLatency = arrivalTimeInMS - inputBuffer.getSourceCreationTimestampInMS().getRawValue();
        bufferLatenciesInMS.emplace(inputBuffer.getSequenceNumber(), bufferLatency);
        if (inputBuffer.getSequenceNumber() == INITIAL<SequenceNumber>)
        {
            timestampOfFirstSN = inputBuffer.getSourceCreationTimestampInMS().getRawValue();
        }
        if (timestampOfLastSN.first < inputBuffer.getSequenceNumber())
        {
            timestampOfLastSN = std::make_pair(inputBuffer.getSequenceNumber(), arrivalTimeInMS);
        }
    }
}

DescriptorConfig::Config MonitoringSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMonitoring>(std::move(config), NAME);
}

SinkValidationRegistryReturnType
SinkValidationGeneratedRegistrar::RegisterMonitoringSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MonitoringSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterMonitoringSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MonitoringSink>(sinkRegistryArguments.sinkDescriptor);
}

}
