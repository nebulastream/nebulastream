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
#ifndef NES_BLOOMFILTER_HPP
#define NES_BLOOMFILTER_HPP

#include <Util/Logger/Logger.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/mman.h>

namespace NES::Runtime::Execution {
namespace detail {
    template<typename T = void>
    T* allocAligned(size_t size, size_t alignment = 16) {
        void* tmp = nullptr;
        NES_ASSERT2_FMT(0 == posix_memalign(&tmp, alignment, sizeof(T) * size),
                        "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
        return reinterpret_cast<T*>(tmp);
    }

    template<typename T = void, size_t huge_page_size = 1 << 21>
    T* allocHugePages(size_t size) {
        void* tmp = nullptr;
        NES_ASSERT2_FMT(0 == posix_memalign(&tmp, huge_page_size, sizeof(T) * size),
                        "Cannot allocate " << sizeof(T) * size << " bytes: " << strerror(errno));
        madvise(tmp, size * sizeof(T), MADV_HUGEPAGE);
        NES_ASSERT2_FMT(tmp != nullptr, "Cannot remap as huge pages");
        mlock(tmp, size * sizeof(T));
        //    uint8_t* ptr = reinterpret_cast<uint8_t*>(tmp);
        //    for (size_t i = 0, len = size * sizeof(T); i < len; i += huge_page_size) {
        //        *(ptr + i) = i;
        //    }
        return reinterpret_cast<T*>(tmp);
    }
} // namespace detail


namespace Operators {

/**
 * @brief A bloom filter that works with 64-bit keys
 */
class alignas(64) BloomFilter {

  public:
    /**
     * @brief Creates a bloom filter for the expected no. entries and the false positive rate
     * @param entries
     * @param falsePositiveRate
     */
    explicit BloomFilter(uint64_t entries, double falsePositiveRate);

    /**
     * @brief adds a hash to the bloom filter
     * @param hash
     */
    void add(uint64_t hash);

    /**
     * @brief checks if the key is in the bloom filter
     * @param hash
     * @return true and false
     */
    bool contains(uint64_t key);

    /**
     * @brief destructs the BloomFilter
     */
    ~BloomFilter();

  private:
    uint32_t noBits;
    uint16_t noHashes;
    uint8_t* bitField;
};

} // namespace NES::Runtime::Execution::Operator
} // namespace NES::Runtime::Execution
#endif//NES_BLOOMFILTER_HPP
