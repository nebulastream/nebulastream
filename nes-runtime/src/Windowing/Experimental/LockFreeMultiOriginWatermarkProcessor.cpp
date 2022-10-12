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
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
namespace NES::Experimental {

LockFreeMultiOriginWatermarkProcessor::LockFreeMultiOriginWatermarkProcessor(const std::vector<OriginId> origins)
    : origins(std::move(origins)) {
    for (uint64_t i = 0; i < this->origins.size(); i++) {
        watermarkProcessors.emplace_back(std::make_shared<Util::NonBlockingMonotonicSeqQueue<OriginId>>());
    }
};

std::shared_ptr<LockFreeMultiOriginWatermarkProcessor>
LockFreeMultiOriginWatermarkProcessor::create(const std::vector<OriginId> origins) {
    return std::make_shared<LockFreeMultiOriginWatermarkProcessor>(origins);
}

uint64_t LockFreeMultiOriginWatermarkProcessor::updateWatermark(uint64_t ts, uint64_t sequenceNumber, OriginId origin) {
    for (size_t originIndex = 0; originIndex < origins.size(); originIndex++) {
        if (origins[originIndex] == origin) {
            watermarkProcessors[originIndex]->emplace(sequenceNumber, ts);
        }
    }
    return getCurrentWatermark();
}

uint64_t LockFreeMultiOriginWatermarkProcessor::getCurrentWatermark() {
    auto minimalWatermark = UINT64_MAX;
    for (auto& wt : watermarkProcessors) {
        minimalWatermark = std::min(minimalWatermark, wt->getCurrentValue());
    }
    return minimalWatermark;
}

}// namespace NES::Experimental