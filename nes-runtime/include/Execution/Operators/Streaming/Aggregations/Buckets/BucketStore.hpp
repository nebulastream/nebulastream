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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_BUCKETSTORE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_BUCKETSTORE_HPP_

#include <Execution/Operators/Streaming/Aggregations/WindowProcessingException.hpp>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Util/Logger/Logger.hpp>
#include <list>
#include <map>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class NonKeyedSlice;
using GlobalSlicePtr = std::unique_ptr<NonKeyedSlice>;

/**
* @brief A Slice store for tumbling and sliding windows,
* which stores slices for a specific thread.
* In the current implementation we handle tumbling windows as sliding widows with windowSize==windowSlide.
* As the slice store is only using by a single thread, we dont have to protect its functions for concurrent accesses.
*/
template<class SliceType>
class BucketStore {
  public:
    using SliceTypePtr = std::unique_ptr<SliceType>;
    explicit BucketStore(uint64_t windowSize, uint64_t windowSlide) : windowSize(windowSize), windowSlide(windowSlide){};
    virtual ~BucketStore() = default;

    /**
     * @brief Lookup all buckets that capture a specific timestamp ts
     * @param ts
     * @return std::vector<SliceType*>*
     */
    std::vector<SliceType*>* findBucketsByTs(uint64_t ts) {
        if (ts < lastWatermarkTs) {
            throw WindowProcessingException("The ts " + std::to_string(ts) + " can't be smaller then the lastWatermarkTs "
                                            + std::to_string(lastWatermarkTs));
        }
        // get a read lock
        std::lock_guard<std::mutex> lock(mutex);
        localResultVector.clear();
        int64_t timestamp = ts;

        int64_t remainder = (timestamp) % windowSize;
        // handle both positive and negative cases
        int64_t lastStart;
        if (remainder < 0) {
            lastStart = timestamp - (remainder + windowSize);
        } else {
            lastStart = timestamp - remainder;
        }
        // iterate over all windows that cover the ts
        for (int64_t start = lastStart; start >= 0 && start > timestamp - windowSize; start -= windowSlide) {
            auto bucketRef = buckets.find(start);
            if (bucketRef == buckets.end()) {
                auto bucket = allocateNewSlice(start, start + windowSize);
                localResultVector.emplace_back(bucket.get());
                buckets.emplace(start, std::move(bucket));
            } else {
                auto& bucket = bucketRef->second;
                localResultVector.emplace_back(bucket.get());
            }
        }
        return &localResultVector;
    }

    /**
     * @brief Extracts all buckets from the bucket store that end before timestamp
     * @param ts
     * @return
     */
    std::list<std::shared_ptr<SliceType>> extractBucketsUntilTs(uint64_t ts) {
        std::lock_guard<std::mutex> lock(mutex);
        // drop all slices as long as the list is not empty and the first slice ends before or at the current ts.
        std::list<std::shared_ptr<SliceType>> resultBuckets;
        for (auto i = buckets.begin(), last = buckets.end(); i != last && i->first + windowSize <= ts;) {
            resultBuckets.push_back(std::move(i->second));
            i = buckets.erase(i);
        }
        return resultBuckets;
    }

    /**
     * @brief Returns the last watermark.
     * @return uint64_t
     */
    uint64_t getLastWatermark() { return lastWatermarkTs; }

    /**
     * @brief Sets the last watermark
     * @param watermarkTs
     */
    void setLastWatermark(uint64_t watermarkTs) { this->lastWatermarkTs = watermarkTs; }

    /**
     * @brief Returns the number of currently stored slices
     * @return uint64_t
     */
    uint64_t getNumberOfSlices() { return buckets->size(); }

    /**
     * @brief Gets an unprotected reference to all slices
     * @return std::list<KeyedSlicePtr>
     */
    auto& getSlices() { return buckets; }

  private:
    virtual SliceTypePtr allocateNewSlice(uint64_t startTs, uint64_t endTs) = 0;

  private:
    // use int64_t here because window could start at negative start points.
    int64_t windowSize;
    int64_t windowSlide;
    std::map<uint64_t, SliceTypePtr> buckets;
    std::atomic<uint64_t> lastWatermarkTs = 0;
    std::vector<SliceType*> localResultVector;
    std::mutex mutex;
};
}// namespace NES::Runtime::Execution::Operators

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_BUCKETSTORE_HPP_
