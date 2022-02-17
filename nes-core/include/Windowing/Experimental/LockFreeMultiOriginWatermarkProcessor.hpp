#ifndef NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEMULTIORIGINWATERMARKPROCESSOR_HPP_
#define NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEMULTIORIGINWATERMARKPROCESSOR_HPP_
#include <Windowing/Experimental/LockFreeWatermarkProcessor.hpp>
namespace NES::Experimental {

/**
 * @brief A multi origin version of the lock free watermark processor.
 */
class LockFreeMultiOriginWatermarkProcessor {
  public:
    explicit LockFreeMultiOriginWatermarkProcessor(const uint64_t numberOfOrigins) : numberOfOrigins(numberOfOrigins) {
        for (uint64_t i = 0; i < numberOfOrigins; i++) {
            watermarkProcessors.emplace_back(std::make_shared<LockFreeWatermarkProcessor<>>());
        }
    };

    /**
     * @brief Creates a new watermark processor, for a specific number of origins.
     * @param numberOfOrigins
     */
    static std::shared_ptr<LockFreeMultiOriginWatermarkProcessor> create(const uint64_t numberOfOrigins) {
        return std::make_shared<LockFreeMultiOriginWatermarkProcessor>(numberOfOrigins);
    }

    /**
     * @brief Updates the watermark timestamp and origin and emits the current watermark.
     * @param ts watermark timestamp
     * @param sequenceNumber
     * @return currentWatermarkTs
     */
    uint64_t updateWatermark(uint64_t ts, uint64_t sequenceNumber, uint64_t origin) {
        if (!map.contains(origin)) {
            auto table = map.lock_table();
            auto [_, status] = table.insert(origin, currentOrigins);
            if (status) {
                currentOrigins++;
            }
            table.unlock();
        }
        auto index = map.find(origin);
        watermarkProcessors[index]->updateWatermark(ts, sequenceNumber);
        return getCurrentWatermark();
    }

    /**
     * @brief Returns the current watermark across all origins
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getCurrentWatermark() {
        if (currentOrigins < numberOfOrigins) {
            return 0;
        }
        auto minWt = UINT64_MAX;

        for (auto& wt : watermarkProcessors) {
            minWt = std::min(minWt, wt->getCurrentWatermark());
        }

        return minWt;
    }

  private:
    cuckoohash_map<uint64_t, uint64_t> map;
    const uint64_t numberOfOrigins;
    std::vector<std::shared_ptr<LockFreeWatermarkProcessor<>>> watermarkProcessors;
    uint64_t currentOrigins = 0;
};

}

#endif//NES_NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_LOCKFREEMULTIORIGINWATERMARKPROCESSOR_HPP_
