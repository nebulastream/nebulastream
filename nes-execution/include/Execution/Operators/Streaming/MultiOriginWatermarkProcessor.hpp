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
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <cstdint>
#include <memory>
#include <vector>

namespace NES::Runtime::Execution::Operators {

using OriginId = uint64_t;
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
     * @param sequenceNumber of the watermark ts
     * @param origin of the watermark ts
     * @return currentWatermarkTs
     */
    uint64_t updateWatermark(uint64_t ts, uint64_t sequenceNumber, OriginId origin);

    /**
     * @brief Returns the current watermark across all origins
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getCurrentWatermark();

    std::string getCurrentStatus();

  private:
    const std::vector<OriginId> origins;
    std::vector<std::shared_ptr<NES::Util::NonBlockingMonotonicSeqQueue<OriginId>>> watermarkProcessors = {};
};

}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_MULTIORIGINWATERMARKPROCESSOR_HPP_
