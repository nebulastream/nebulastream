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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MULTIORIGINWATERMARKPROCESSOR_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MULTIORIGINWATERMARKPROCESSOR_HPP_

#include <map>
#include <memory>
#include <mutex>

namespace NES {
using WatermarkTs = uint64_t;
using OriginId = uint64_t;
using SequenceNumber = uint64_t;
}// namespace NES

namespace NES::Windowing {

class WatermarkProcessor;

/**
 * @brief The watermark processor receives watermark barriers and provides the current watermark across multiple origins.
 * The watermark processor guarantees strict serializable watermark updates.
 * Thus, a watermark processor is only executed if all preceding updates have been processed.
 * Consequently, the watermark processor expects a exactly once delivery on the input channel.
 *
 * If the watermark processor receives the following barriers for origin 1:
 * <sequenceNr, watermarkTs>
 * <1,1>, <2,2>, <4,4>, <5,5>, <3,3>, <6,6>
 *
 * It will provide the following watermarks:
 * <1>, <2>, <2>, <2>, <5>, <6>
 *
 */
class MultiOriginWatermarkProcessor {
  public:
    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param numberOfOrigins
     */
    explicit MultiOriginWatermarkProcessor(const uint64_t numberOfOrigins);

    ~MultiOriginWatermarkProcessor();

    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param numberOfOrigins
     */
    static std::shared_ptr<MultiOriginWatermarkProcessor> create(uint64_t numberOfOrigins);

    /**
     * @brief Processes a watermark barrier.
     * @param watermarkBarrier
     */
    void updateWatermark(WatermarkTs ts, SequenceNumber sequenceNumber, OriginId origin);

 bool isWatermarkSynchronized(OriginId originId) const;

    /**
     * @brief Returns the visible watermark across all origins.
     * @return WatermarkTs
     */
    [[nodiscard]] WatermarkTs getCurrentWatermark() const;

    /**
     * @brief Returns success if there are no tuples with smaller sequence number that haven't arrived yet than the last tuple seen
     * @param originId origin id
     * @return Success
     */
    bool isWatermarkSynchronized(OriginId originId) const;

  private:
    mutable std::mutex watermarkLatch;
    const uint64_t numberOfOrigins;
    // The watermark processor maintains a local watermark processor for each origin.
    std::map<uint64_t, std::unique_ptr<WatermarkProcessor>> localWatermarkProcessor;
};
using MultiOriginWatermarkProcessorPtr = std::unique_ptr<MultiOriginWatermarkProcessor>;
}// namespace NES::Windowing

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_MULTIORIGINWATERMARKPROCESSOR_HPP_
