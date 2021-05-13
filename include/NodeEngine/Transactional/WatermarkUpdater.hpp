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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKUPDATER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKUPDATER_HPP_

#include <NodeEngine/Transactional/LocalWatermarkUpdater.hpp>
#include <NodeEngine/Transactional/WatermarkBarrier.hpp>
#include <memory>
namespace NES::NodeEngine::Transactional {

/**
 * @brief The watermark updater receives watermark barriers and provides the current watermark across multiple origins.
 * The watermark updater guarantees a strict serializable for watermark updates. Thus, a watermark update is only executed if all
 * preceding updates have been processed.
 * Consequently, the watermark updater expects a exactly once delivery on the input channel.
 */
class WatermarkUpdater {
  public:
    /**
     * @brief Creates a new watermark updater, with a specific origin id.
     * @param originId
     */
    WatermarkUpdater(const uint64_t numberOfOrigins);

    static std::shared_ptr<WatermarkUpdater> create(const uint64_t numberOfOrigins);

    /**
     * @brief Processes a watermark barrier
     * @param watermarkBarrier
     */
    void updateWatermark(WatermarkBarrier watermarkBarrier);

    /**
     * @brief Generates a new watermark
     * @return
     */
    WatermarkTs getCurrentWatermark();

  private:
    std::mutex updateLatch;
    const uint64_t numberOfOrigins;
    std::vector<std::unique_ptr<LocalWatermarkUpdater>> watermarkManagers;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKEMITTER_HPP_
