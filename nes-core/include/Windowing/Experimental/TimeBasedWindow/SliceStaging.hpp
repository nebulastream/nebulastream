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
        Partition(std::unique_ptr<std::vector<Runtime::TupleBuffer>> buffers) : buffers(std::move(buffers)){};
        std::unique_ptr<std::vector<Runtime::TupleBuffer>> buffers;
    };

    uint64_t addToSlice(uint64_t sliceIndex, Partition partition) {
        const std::lock_guard<std::mutex> lock(sliceStagingMutex);
        if (!slicePartitionMap.contains(sliceIndex)) {
            slicePartitionMap[sliceIndex] = std::make_unique<std::vector<Partition>>();
        }
        slicePartitionMap[sliceIndex]->emplace_back(std::move(partition));
        return slicePartitionMap[sliceIndex]->size();
    }

    std::unique_ptr<std::vector<Partition>> erasePartition(uint64_t sliceIndex) {
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
    std::map<uint64_t, std::unique_ptr<std::vector<Partition>>> slicePartitionMap;
};
}// namespace NES::Windowing::Experimental

#endif//NES_INCLUDE_WINDOWING_EXPERIMENTAL_TIMEBASEDWINDOW_SLICESTAGING_HPP_
