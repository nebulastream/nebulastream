
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
namespace NES::Experimental {

LockFreeMultiOriginWatermarkProcessor::LockFreeMultiOriginWatermarkProcessor(const std::vector<OriginId> origins)
    : origins(std::move(origins)) {
    for (uint64_t i = 0; i < origins.size(); i++) {
        watermarkProcessors.emplace_back(std::make_shared<Util::NonBlockingMonotonicSeqQueue<OriginId>>());
    }
};

std::shared_ptr<LockFreeMultiOriginWatermarkProcessor> LockFreeMultiOriginWatermarkProcessor::create(const std::vector<OriginId> origins) {
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

uint64_t LockFreeMultiOriginWatermarkProcessor::getCurrentWatermark()  {
    auto minimalWatermark = UINT64_MAX;
    for (auto& wt : watermarkProcessors) {
        minimalWatermark = std::min(minimalWatermark, wt->getCurrentValue());
    }
    return minimalWatermark;
}


}// namespace NES::Experimental