/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKPROCESSOR_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKPROCESSOR_HPP_

#include <NodeEngine/Transactional/LocalWatermarkProcessor.hpp>
#include <NodeEngine/Transactional/WatermarkBarrier.hpp>
#include <memory>
namespace NES::NodeEngine::Transactional {

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
class WatermarkProcessor {
  public:
    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param numberOfOrigins
     */
    WatermarkProcessor(const uint64_t numberOfOrigins);

    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param numberOfOrigins
     */
    static std::shared_ptr<WatermarkProcessor> create(const uint64_t numberOfOrigins);

    /**
     * @brief Processes a watermark barrier.
     * @param watermarkBarrier
     */
    void updateWatermark(WatermarkBarrier watermarkBarrier);

    /**
     * @brief Returns the visible watermark across all origins.
     * @return WatermarkTs
     */
    [[nodiscard]] WatermarkTs getCurrentWatermark() const;

  private:
    const uint64_t numberOfOrigins;
    // The watermark processor maintains a local watermark processor for each origin.
    std::vector<std::unique_ptr<LocalWatermarkProcessor>> localWatermarkProcessor;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKEMITTER_HPP_
