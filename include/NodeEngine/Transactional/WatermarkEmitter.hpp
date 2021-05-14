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

#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKEMITTER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKEMITTER_HPP_

#include <NodeEngine/Transactional/WatermarkBarrier.hpp>
#include <memory>
#include <mutex>
namespace NES::NodeEngine::Transactional {

/**
 * @brief The watermark emitter maintains the current max watermark and emits watermark barriers to downstream operators.
 * Furthermore, it requires that users provide a valid watermark update order.
 */
class WatermarkEmitter {
  public:
    /**
     * @brief Creates a new watermark emitter, with a specific origin id.
     * @param originId
     */
    WatermarkEmitter(const OriginId originId);

    /**
    * @brief Creates a new watermark emitter, with a specific origin id.
    * @param originId
    */
    static std::shared_ptr<WatermarkEmitter> create(const OriginId originId);

    /**
     * @brief Updates the local watermark and indicates that the watermark was changed.
     * This assumes that the watermarkTs increases in an strictly monotonic order.
     * @param watermarkTs
     * @return true if the watermark was changed.
     */
    bool updateWatermark(uint64_t watermarkTs);

    /**
     * @brief Generates a new watermark
     * @return
     */
    WatermarkBarrier getNextWatermarkBarrier();

  private:
    mutable std::mutex emitLatch;
    const OriginId originId;
    WatermarkTs currentWatermark;
    BarrierSequenceNumber currentSequenceNumber;
};

}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKEMITTER_HPP_
