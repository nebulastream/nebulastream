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
#ifndef NES_FIXEDPAGE_HPP
#define NES_FIXEDPAGE_HPP

#include <atomic>

#include <Nautilus/Interface/Record.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/BloomFilter.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief class that stores the tuples on a page.
 * It also contains a bloom filter to have a quick check if a tuple is not on the page
 */
class FixedPage{
  public:
    static constexpr auto CHUNK_SIZE = 4 * 1024;
    static constexpr auto BLOOM_FALSE_POSITIVE_RATE = 1e-2;

    FixedPage(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord);

    /**
     * @brief returns the record at the given index
     * @param index
     * @return Record
     */
    Nautilus::Record& operator[](size_t index) const;

    /**
     * @brief appends the record to this page and checks if there is enough space for another record
     * @param hash
     * @param record
     * @return still capacity for another record
     */
    bool append(const uint64_t hash, const Nautilus::Record& record);

    /**
     * @brief checks if the key might be in this page
     * @param key
     * @return true or false
     */
    bool bloomFilterCheck(uint64_t key) const;

  private:
    Nautilus::Record* data;
    size_t pos;
    size_t capacity;
    std::unique_ptr<BloomFilter> bloomFilter;
};

} // namespace NES::Runtime::Execution::Operators
#endif//NES_FIXEDPAGE_HPP
