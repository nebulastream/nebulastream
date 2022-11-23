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

#ifndef NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#define NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
#include <Exceptions/WindowProcessingException.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSlice.hpp>
#include <Windowing/Experimental/ThreadLocalSliceStore.hpp>
#include <memory>

namespace NES::Experimental {
class HashMapFactory;
using HashMapFactoryPtr = std::shared_ptr<HashMapFactory>;
}// namespace NES::Experimental

namespace NES::Windowing::Experimental {

class KeyedSlice;
using KeyedSlicePtr = std::unique_ptr<KeyedSlice>;

/**
 * @brief A Slice store for tumbling and sliding windows,
 * which stores slices for a specific thread.
 * In the current implementation we handle tumbling widows as sliding widows with windowSize==windowSlide.
 * As the slice store is only used by a single thread, we dont have to protect its functions for concurrent accesses.
 */
class KeyedThreadLocalSliceStore : public ThreadLocalSliceStore<KeyedSlice> {
  public:
    explicit KeyedThreadLocalSliceStore(NES::Experimental::HashMapFactoryPtr hashMapFactory,
                                        uint64_t windowSize,
                                        uint64_t windowSlide);
    ~KeyedThreadLocalSliceStore() = default;

  private:
    KeyedSlicePtr allocateNewSlice(uint64_t startTs, uint64_t endTs) override;
    NES::Experimental::HashMapFactoryPtr hashMapFactory;
};

}// namespace NES::Windowing::Experimental
#endif// NES_CORE_INCLUDE_WINDOWING_EXPERIMENTAL_KEYEDTIMEWINDOW_KEYEDTHREADLOCALSLICESTORE_HPP_
