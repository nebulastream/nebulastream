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

#include <CodecMonitoringSink.hpp>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Encoders/Encoder.hpp>
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
struct CompressionRateMeasurements
{
    double minRate = 0;
    double maxRate = std::numeric_limits<double>::max();
    double avgRate = 0;

    friend std::ostream& operator<<(std::ostream& os, const CompressionRateMeasurements& rateMeasurements)
    {
        return os << fmt::format(
                   "average rate, min rate, max rate\n{},{},{}",
                   rateMeasurements.minRate,
                   rateMeasurements.maxRate,
                   rateMeasurements.avgRate);
    }
};

struct CompressionRatioMeasurements
{
    double minRatio = 0;
    double maxRatio = std::numeric_limits<double>::max();
    double avgRatio = 0;

    friend std::ostream& operator<<(std::ostream& os, const CompressionRatioMeasurements& ratioMeasurements)
    {
        return os << fmt::format(
                   "average ratio, min ratio, max ratio\n{},{},{}",
                   ratioMeasurements.minRatio,
                   ratioMeasurements.maxRatio,
                   ratioMeasurements.avgRatio);
    }
};
}

FMT_OSTREAM(NES::CompressionRateMeasurements);
FMT_OSTREAM(NES::CompressionRatioMeasurements);

namespace NES
{

CodecMonitoringSink::CodecMonitoringSink(
    BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor, std::unique_ptr<Encoder> encoder)
    : Sink(std::move(backpressureController))
    , isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersCodecMonitoring::FILE_PATH))
    , encoder(std::move(encoder))
    , tupleSize(sinkDescriptor.getSchema()->getSizeOfSchemaInBytes())
{
}

void CodecMonitoringSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up codec monitoring sink: {}", *this);
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

void CodecMonitoringSink::stop(PipelineExecutionContext&)
{
    NES_WARNING("Stopping codec monitoring sink");
    if (this->encodingRates.empty())
    {
        NES_WARNING("Encodingrates empty");
        return;
    }

    const auto ratioMeasurements = [](const std::vector<double>& ratios)
    {
        double minRatio = std::numeric_limits<double>::max();
        double maxRatio = 0;
        double ratioSum = 0;
        for (const auto ratio : ratios)
        {
            minRatio = (ratio != 0) ? std::min(ratio, minRatio) : minRatio;
            maxRatio = std::max(ratio, maxRatio);
            ratioSum += ratio;
        }
        return CompressionRatioMeasurements{
            .minRatio = minRatio, .maxRatio = maxRatio, .avgRatio = ratioSum / static_cast<double>(ratios.size())};
    };

    const auto rateMeasurements = [](const std::vector<double>& rates)
    {
        double minRate = std::numeric_limits<double>::max();
        double maxRate = 0;
        double rateSum = 0;
        for (const auto rate : rates)
        {
            minRate = (rate != 0) ? std::min(rate, minRate) : minRate;
            maxRate = std::max(rate, maxRate);
            rateSum += rate;
        }
        return CompressionRateMeasurements{.minRate = minRate, .maxRate = maxRate, .avgRate = rateSum / static_cast<double>(rates.size())};
    };

    CompressionRateMeasurements childRateMeasurements;
    CompressionRatioMeasurements childRatioMeasurements;
    if (this->childCompressionRatios.empty())
    {
        childRateMeasurements = CompressionRateMeasurements{std::numeric_limits<double>::max(), 0, 0};
        childRatioMeasurements = CompressionRatioMeasurements{std::numeric_limits<double>::max(), 0, 0};
    }
    else
    {
        childRateMeasurements = rateMeasurements(childEncodingRates);
        childRatioMeasurements = ratioMeasurements(childCompressionRatios);
    }
    CompressionRateMeasurements mainRateMeasurements = rateMeasurements(encodingRates);
    CompressionRatioMeasurements mainRatioMeasurements = ratioMeasurements(compressionRatios);

    // outputFileStream << fmt::format("Throughput in MB/s {}, {}", throughputInMegaBytesPerSecond, latencyMeasurements);
    outputFileStream << fmt::format(
        "main ratio min,main ratio max,main ratio avg,main rate min,main rate max,main rate avg,child ratio min,child ratio max,child "
        "ratio avg,child rate min,child rate max,child rate avg\n{},{},{},{},{},{},{},{},{},{},{},{}\n",
        mainRatioMeasurements.minRatio,
        mainRatioMeasurements.maxRatio,
        mainRatioMeasurements.avgRatio,
        mainRateMeasurements.minRate,
        mainRateMeasurements.maxRate,
        mainRateMeasurements.avgRate,
        childRatioMeasurements.minRatio,
        childRatioMeasurements.maxRatio,
        childRatioMeasurements.avgRatio,
        childRateMeasurements.minRate,
        childRateMeasurements.maxRate,
        childRateMeasurements.avgRate);
    outputFileStream.close();
    isOpen = false;
}

void CodecMonitoringSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in CodecMonitoringSink.");

    for (size_t childIdx = 0; childIdx < inputBuffer.getNumberOfChildBuffers(); ++childIdx)
    {
        auto childBuffer = inputBuffer.loadChildBuffer(VariableSizedAccess::Index(childIdx));
        const std::span childMemory(childBuffer.getAvailableMemoryArea<>().data(), childBuffer.getNumberOfTuples());
        std::vector<char> encodedChild{};

        auto start = std::chrono::steady_clock::now();
        const auto encodingResult = encoder->encodeBuffer(childMemory, encodedChild);
        auto end = std::chrono::steady_clock::now();

        if (encodingResult.status == Encoder::EncodeStatusType::ENCODING_ERROR)
        {
            NES_ERROR("Error during encoding in encoder sink")
        }
        const std::chrono::duration<double> elapsed = end - start;
        const double timeInSeconds = elapsed.count();

        const double rate = childBuffer.getNumberOfTuples() / timeInSeconds;
        const double ratio = static_cast<double>(childBuffer.getNumberOfTuples()) / encodingResult.compressedSize;
        {
            const std::scoped_lock lock(mutex);
            childCompressionRatios.emplace_back(ratio);
            childEncodingRates.emplace_back(rate);
        }
    }

    const std::span usedBufferMemory(inputBuffer.getAvailableMemoryArea<>().data(), inputBuffer.getNumberOfTuples() * tupleSize);
    std::vector<char> encodedBuffer{};

    auto start = std::chrono::steady_clock::now();
    const auto encodingResult = encoder->encodeBuffer(usedBufferMemory, encodedBuffer);
    auto end = std::chrono::steady_clock::now();
    if (encodingResult.status == Encoder::EncodeStatusType::ENCODING_ERROR)
    {
        NES_ERROR("Error during encoding in encoder sink")
    }
    const std::chrono::duration<double> elapsed = end - start;
    const double timeInSeconds = elapsed.count();

    const double rate = inputBuffer.getNumberOfTuples() * tupleSize / timeInSeconds;
    const double ratio = static_cast<double>(inputBuffer.getNumberOfTuples() * tupleSize) / encodingResult.compressedSize;
    {
        const std::scoped_lock lock(mutex);
        compressionRatios.emplace_back(ratio);
        encodingRates.emplace_back(rate);
    }
}

DescriptorConfig::Config CodecMonitoringSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersCodecMonitoring>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterCodecMonitoringSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return CodecMonitoringSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterCodecMonitoringSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<CodecMonitoringSink>(
        std::move(sinkRegistryArguments.backpressureController),
        sinkRegistryArguments.sinkDescriptor,
        std::move(sinkRegistryArguments.encoder.value()));
}
}
