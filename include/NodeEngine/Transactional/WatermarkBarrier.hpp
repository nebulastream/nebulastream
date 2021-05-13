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
#ifndef NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKBARRIER_HPP_
#define NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKBARRIER_HPP_
#include <cstdint>
namespace NES::NodeEngine::Transactional {

using WatermarkTs = uint64_t;
using OriginId = uint64_t;
using BarrierSequenceNumber = uint64_t;

/**
 * @brief A watermark barrier, which consists of the watermark timestamp, the sequence number, and the origin id.
 */
class WatermarkBarrier {
  public:
    WatermarkBarrier(WatermarkTs ts, BarrierSequenceNumber sequenceNumber, OriginId origin);

    /**
     * @brief Gets the watermark timestamp
     * @return WatermarkTs
     */
    WatermarkTs getTs() const;

    /**
     * @brief Gets the sequence number
     * @return BarrierSequenceNumber
     */
    BarrierSequenceNumber getSequenceNumber() const;

    /**
     * @brief Gets the origin id
     * @return OriginId
     */
    OriginId getOrigin() const;

  private:
    WatermarkTs ts;
    BarrierSequenceNumber sequenceNumber;
    OriginId origin;
};
}// namespace NES::NodeEngine::Transactional

#endif//NES_INCLUDE_NODEENGINE_TRANSACTIONAL_WATERMARKBARRIER_HPP_
