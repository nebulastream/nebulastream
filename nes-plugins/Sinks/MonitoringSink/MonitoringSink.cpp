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

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <ranges>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <SinksParsing/NoneFormat.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{
struct LatencyMeasurements
{
    uint64_t minLatency = 0;
    uint64_t maxLatency = 0;
    double avgLatency = 0;
    uint64_t p50 = 0;
    uint64_t p99 = 0;
    uint64_t p999 = 0;

    friend std::ostream& operator<<(std::ostream& os, const LatencyMeasurements& latencyMeasurements)
    {
        return os << fmt::format(
                   "average latency, min latency, max latency, p50, p99, p999\n{},{},{},{},{},{}",
                   latencyMeasurements.avgLatency,
                   latencyMeasurements.minLatency,
                   latencyMeasurements.maxLatency,
                   latencyMeasurements.p50,
                   latencyMeasurements.p99,
                   latencyMeasurements.p999);
    }
};
}

FMT_OSTREAM(NES::LatencyMeasurements);

namespace NES
{

MonitoringSink::MonitoringSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersMonitoring::FILE_PATH))
    // Todo: needs to be fileSize * repetitions * numClients
    , sizeOfInputDataInBytes(sinkDescriptor.getFromConfig(ConfigParametersMonitoring::SIZE_OF_INPUT_DATA_IN_BYTES))
    // , sizeOfInputDataInBytes(497214231)
    , timestampOfLastSN(std::make_pair(INVALID<SequenceNumber>, 0))
{
    const auto legacyOutputFormat = toUpperCase(sinkDescriptor.getFromConfig(ConfigParametersMonitoring::LEGACY_OUTPUT_FORMAT));
    if (legacyOutputFormat == "CSV")
    {
        format = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
    }
    else if (legacyOutputFormat == "JSON")
    {
        format = std::make_unique<JSONFormat>(*sinkDescriptor.getSchema());
    }
    else
    {
        format = std::make_unique<NoneFormat>(*sinkDescriptor.getSchema());
    }
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
    if (this->bufferLatenciesInUS.empty())
    {
        NES_WARNING("Latencies empty");
        return;
    }

    const auto latencyMeasurements = [](const std::unordered_map<SequenceNumber, uint64_t>& bufferLatencies)
    {
        /// Collect all per-buffer latencies and sort once: avg/min/max AND the percentiles
        /// (p50/p99/p999) all come from the same sorted vector -- every sample is retained
        /// (no reservoir), so the percentiles are exact for this query.
        std::vector<uint64_t> sorted;
        sorted.reserve(bufferLatencies.size());
        uint64_t latencySum = 0;
        for (const auto latency : bufferLatencies | std::views::values)
        {
            sorted.push_back(latency);
            latencySum += latency;
        }
        std::ranges::sort(sorted);
        const auto n = sorted.size();
        /// nearest-rank percentile over the sorted samples (clamped). p in [0,1].
        const auto pct = [&](const double p) -> uint64_t
        {
            if (n == 0)
            {
                return 0;
            }
            const auto idx = static_cast<size_t>((p * static_cast<double>(n - 1)) + 0.5);
            return sorted[std::min(idx, n - 1)];
        };
        /// min excludes spurious 0s (same-us stamps); avg/percentiles include every sample.
        uint64_t minLatency = 0;
        for (const auto v : sorted)
        {
            if (v != 0)
            {
                minLatency = v;
                break;
            }
        }
        return LatencyMeasurements{
            .minLatency = minLatency,
            .maxLatency = (n != 0) ? sorted.back() : 0,
            .avgLatency = (n != 0) ? latencySum / static_cast<double>(n) : 0.0,
            .p50 = pct(0.50),
            .p99 = pct(0.99),
            .p999 = pct(0.999)};
    }(this->bufferLatenciesInUS);

    // Todo: is wrong for repititions
    /// timestamps are in us now: lastArrival - firstReadStart = ingestion-inclusive e2e span.
    const auto processingTimeInSeconds = (this->timestampOfLastSN.second - this->timestampOfFirstSN) / static_cast<double>(1000000);
    const auto inputSizeInMegaBytes = this->sizeOfInputDataInBytes / 1000000;
    const auto throughputInMegaBytesPerSecond = inputSizeInMegaBytes / processingTimeInSeconds;
    // outputFileStream << fmt::format("Throughput in MB/s {}, {}", throughputInMegaBytesPerSecond, latencyMeasurements);
    /// first read start / last arrival are ABSOLUTE epoch microseconds (not a per-query span):
    /// pbench takes min(first) and max(last) ACROSS all concurrent queries to recover the true
    /// system wall-span, so aggregate throughput = total bytes / wall-span -- instead of summing
    /// per-query rates over staggered windows (which overcounts past DRAM peak).
    outputFileStream << fmt::format(
        "processing time in s,throughput in MB/s,average latency us,min latency us,max latency us,p50 latency us,p99 "
        "latency us,p999 latency us,first read start us,last arrival us\n{},{},{},{},{},{},{},{},{},{}\n",
        processingTimeInSeconds,
        throughputInMegaBytesPerSecond,
        latencyMeasurements.avgLatency,
        latencyMeasurements.minLatency,
        latencyMeasurements.maxLatency,
        latencyMeasurements.p50,
        latencyMeasurements.p99,
        latencyMeasurements.p999,
        this->timestampOfFirstSN,
        this->timestampOfLastSN.second);
    outputFileStream.close();
    isOpen = false;
}

void MonitoringSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in MonitoringSink.");
    /// Obligatory legacy formatting step
    (void)format->getFormattedBuffer(inputBuffer);

    // if (inputBuffer.isLastChunk())
    /// If the current buffer contains the source creation timestamp marker, measure the latency
    if (inputBuffer.getSourceCreationTimestampInMS().getRawValue() != Timestamp::INITIAL_VALUE)
    {
        const std::scoped_lock lock(mutex);
        /// us resolution: sourceCreationTimestamp is stamped at the source in us (read-start), so this
        /// difference is the true end-to-end latency including the read/recv copy.
        const auto arrivalTimeInUS = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
        const auto bufferLatency = arrivalTimeInUS - inputBuffer.getSourceCreationTimestampInMS().getRawValue();
        bufferLatenciesInUS.emplace(inputBuffer.getSequenceNumber(), bufferLatency);
        if (inputBuffer.getSequenceNumber() == INITIAL<SequenceNumber>)
        {
            timestampOfFirstSN = inputBuffer.getSourceCreationTimestampInMS().getRawValue();
        }
        if (timestampOfLastSN.first < inputBuffer.getSequenceNumber())
        {
            timestampOfLastSN = std::make_pair(inputBuffer.getSequenceNumber(), arrivalTimeInUS);
        }
    }
}

DescriptorConfig::Config MonitoringSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersMonitoring>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterMonitoringSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MonitoringSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterMonitoringSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MonitoringSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}
}
