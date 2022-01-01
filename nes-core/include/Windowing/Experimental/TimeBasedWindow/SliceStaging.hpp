#ifndef NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGING_HPP_
#define NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGING_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <cinttypes>
#include <memory>
#include <mutex>

namespace NES::Windowing::Experimental {

class SliceStaging {
  public:
    class Partition {
      public:
        std::vector<Runtime::TupleBuffer> buffers;
        uint64_t addedSlices = 0;
    };

    std::tuple<uint64_t, uint64_t> addToSlice(uint64_t sliceIndex, std::unique_ptr<std::vector<Runtime::TupleBuffer>> entries) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        if (!slicePartitionMap.contains(sliceIndex)) {
            slicePartitionMap[sliceIndex] = std::make_unique<Partition>();
        }
        auto& partition = slicePartitionMap[sliceIndex];
        for (auto& entryBuffer : (*entries.get())) {
            partition->buffers.emplace_back(entryBuffer);
        }
        partition->addedSlices++;

        return {partition->addedSlices,  partition->buffers.size()};
    }

    std::unique_ptr<Partition> erasePartition(uint64_t sliceIndex) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        if (!slicePartitionMap.contains(sliceIndex)) {
            throw WindowProcessingException("Slice Index " + std::to_string(sliceIndex) + "not available");
        }
        auto value = std::move(slicePartitionMap[sliceIndex]);
        auto iter = slicePartitionMap.find(sliceIndex);
        slicePartitionMap.erase(iter);
        return value;
    }

  private:
    std::mutex sliceStagingMutex;
    std::map<uint64_t, std::unique_ptr<Partition>> slicePartitionMap;
};
}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGING_HPP_
