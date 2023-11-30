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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDBUCKETSTORE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDBUCKETSTORE_HPP_
#include <Execution/Operators/Streaming/Aggregations/Buckets/BucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <memory>

namespace NES::Runtime::Execution::Operators {

class State;
class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;

/**
 * @brief A thread local slice store for global (non-keyed) tumbling and sliding windows,
 * which stores slices for a specific thread.
 * In the current implementation we handle tumbling windows as sliding widows with windowSize==windowSlide.
 * As the slice store is only using by a single thread, we don't have to protect its functions for concurrent accesses.
 */
class KeyedBucketStore : public BucketStore<KeyedSlice> {
  public:
    static const uint64_t DEFAULT_NUMBER_OF_KEYS = 1000;

    /**
     * @brief Constructor to create a new thread local keyed slice store
     * @param keySize size of the key in bytes
     * @param valueSize size of the value in bytes
     * @param windowSize size of the window in ms
     * @param windowSlide size of the window slide in ms
     * @param numberOfKeys number of expected keys
     */
    explicit KeyedBucketStore(uint64_t keySize,
                              uint64_t valueSize,
                              uint64_t windowSize,
                              uint64_t windowSlide,
                              uint64_t numberOfKeys = DEFAULT_NUMBER_OF_KEYS);
    ~KeyedBucketStore() = default;

  private:
    KeyedSlicePtr allocateNewSlice(uint64_t startTs, uint64_t endTs) override;
    const uint64_t keySize;
    const uint64_t valueSize;
    const uint64_t numberOfKeys;
};
}// namespace NES::Runtime::Execution::Operators
#endif  // NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDBUCKETSTORE_HPP_
