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

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <Encoders/EncoderAutoChoice.hpp>

namespace NES::EncoderAutoChoice
{
double
getLogicalThroughput(const double compressionRate, const uint64_t& numberOfThreads, const double& bandwidth, const double& compressionRatio)
{
    const double compressionRateWithThreadsEst = static_cast<double>(numberOfThreads) * compressionRate;
    /// The logical throughput depends on whether the bottleneck is the compression rate or the network bandwidth
    /// For the former case, we need to consider that the network will reeive data that is actually encoded, so the logical bandwidth is potentially higher depending on the compression ratio
    /// The compressiopn rate, on the other hand, is not affected by the compression ratio, as we encode uncompressed data
    return std::min(compressionRateWithThreadsEst, bandwidth / compressionRatio);
}

uint64_t getClosestBufferSizeWithEstimate(const uint64_t& bufferSize)
{
    if (bufferSize <= 24576)
    {
        return 16384;
    }
    if (bufferSize <= 49152)
    {
        return 32768;
    }
    return 65536;
}

std::string chooseCodec(const double& bandwidth, const uint64_t& numberOfWorkerThreads, const uint64_t& bufferSize)
{
    const double physicalThroughput = bandwidth;
    const uint64_t supportedBufferSize = getClosestBufferSizeWithEstimate(bufferSize);
    const double snappyLogicalThroughput = getLogicalThroughput(
        SNAPPY_RATE_ESTIMATE.at(supportedBufferSize), numberOfWorkerThreads, bandwidth, SNAPPY_RATIO_ESTIMATE.at(supportedBufferSize));
    const double lz4LogicalThroughput = getLogicalThroughput(
        LZ4_RATE_ESTIMATE.at(supportedBufferSize), numberOfWorkerThreads, bandwidth, LZ4_RATIO_ESTIMATE.at(supportedBufferSize));
    const double zstdLogicalThroughput = getLogicalThroughput(
        ZSTD_RATE_ESTIMATE.at(supportedBufferSize), numberOfWorkerThreads, bandwidth, ZSTD_RATIO_ESTIMATE.at(supportedBufferSize));
    const std::vector<double> throughputs{physicalThroughput, snappyLogicalThroughput, lz4LogicalThroughput, zstdLogicalThroughput};

    const double maxThroughput = *std::ranges::max_element(throughputs);
    if (maxThroughput == snappyLogicalThroughput)
    {
        return "Snappy";
    }
    if (maxThroughput == zstdLogicalThroughput)
    {
        return "ZSTD";
    }
    if (maxThroughput == lz4LogicalThroughput)
    {
        return "LZ4";
    }
    return "None";
}
}
