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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_MULTIORIGINWATERMARKPROCESSOR_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_MULTIORIGINWATERMARKPROCESSOR_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/NonBlockingMonotonicSeqQueue.hpp>
#include <Util/Common.hpp>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief A multi origin version of the lock free watermark processor.
 */
class MultiOriginWatermarkProcessor {
  public:
    explicit MultiOriginWatermarkProcessor(const std::vector<OriginId>& origins);

    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param origins
     */
    static std::shared_ptr<MultiOriginWatermarkProcessor> create(const std::vector<OriginId>& origins);

    /**
     * @brief Updates the watermark timestamp and origin and emits the current watermark.
     * @param ts watermark timestamp
     * @param sequenceData of the watermark ts
     * @param origin of the watermark ts
     * @return currentWatermarkTs
     */
    uint64_t updateWatermark(uint64_t ts, SequenceData sequenceData, OriginId origin);

    /**
     * @brief Returns the current watermark across all origins
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getCurrentWatermark();

    /**
     * @brief get state of watermarks processor as vector of buffers
     * Format of buffers looks like:
     * buffers contain data in format:
     * -----------------------------------------
     * number of origins (n) | number of states for origin 1 (m)
     * seq number of first block (i_0) | isLastChunk of first block (i_0) | number of chunks of first block (i_0) | ... | same for i_m state
     * same for all other origins j_1 ... j_n
     * @return vector of buffers
     */
    std::vector<Runtime::TupleBuffer> serializeWatermarks(std::shared_ptr<BufferManager> bufferManager) const;

    /**
     * @brief set new timestamps and sequence numbers for all origins
     * Format of buffers looks like:
     * buffers contain data in format:
     * -----------------------------------------
     * number of origins (n) | number of states for origin 1 (m)
     * seq number of first block (i_0) | isLastChunk of first block (i_0) | number of chunks of first block (i_0) | ... | same for i_m state
     * same for all other origins j_1 ... j_n
     * @param vector of buffers
     */
    void restoreWatermarks(std::span<const Runtime::TupleBuffer> buffers);

    std::string getCurrentStatus();

  private:
    const std::vector<OriginId> origins;
    std::vector<std::shared_ptr<NES::Sequencing::NonBlockingMonotonicSeqQueue<uint64_t>>> watermarkProcessors = {};
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_MULTIORIGINWATERMARKPROCESSOR_HPP_
