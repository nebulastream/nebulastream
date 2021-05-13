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

#include <NodeEngine/Transactional/WatermarkEmitter.hpp>
#include <Util/Logger.hpp>
namespace NES::NodeEngine::Transactional {

WatermarkEmitter::WatermarkEmitter(OriginId originId) : originId(originId), currentWatermark(0), currentSequenceNumber(0) {}

std::shared_ptr<WatermarkEmitter> WatermarkEmitter::create(const OriginId originId) {
    return std::make_shared<WatermarkEmitter>(originId);
}

bool WatermarkEmitter::updateWatermark(uint64_t watermarkTs) {
    std::scoped_lock lock(emitLatch);
    NES_ASSERT2_FMT(watermarkTs >= currentWatermark,
                    "We cant update the watermark, to a lower watermark the the current one. "
                    "current watermark:"
                        << currentWatermark << " new watermark: " << watermarkTs);
    if (watermarkTs > currentWatermark) {
        currentWatermark = watermarkTs;
        return true;
    }
    return false;
}

WatermarkBarrier WatermarkEmitter::getNextWatermarkBarrier() {
    std::scoped_lock lock(emitLatch);
    return WatermarkBarrier(currentWatermark, ++currentSequenceNumber, originId);
}

}// namespace NES::NodeEngine::Transactional