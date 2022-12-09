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
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinUtil.hpp>


namespace NES::Runtime::Execution::Operators {

/**
 * @brief class that stores the tuples on a page.
 * It also contains a bloom filter to have a quick check if a tuple is not on the page
 */
class FixedPage{
  public:
    explicit FixedPage(std::atomic<uint64_t>& tail, uint64_t overrunAddress, size_t sizeOfRecord, size_t pageSize);

    FixedPage() = default;

    FixedPage(FixedPage* page);

    FixedPage(const FixedPage& that);

    FixedPage(FixedPage&& that);

    FixedPage& operator=(FixedPage&& that);

    /**
     * @brief returns a pointer to the record at the given index
     * @param index
     * @return pointer to the record
     */
    uint8_t* operator[](size_t index) const;

    /**
     * @brief returns a pointer to a memory location on this page where to write the record and checks if there is enough space for another record
     * @param hash
     * @return null pointer if there is no more space left on the page, otherwise the pointer
     */
    uint8_t* append(const uint64_t hash);

    /**
     * @brief checks if the key might be in this page
     * @param keyPtr
     * @return true or false
     */
    bool bloomFilterCheck(uint8_t* keyPtr, size_t sizeOfKey) const;

    /**
     * @brief returns the number of items on this page
     * @return no. items
     */
    size_t size() const;

  private:
    void swap(FixedPage& lhs, FixedPage& rhs) noexcept;

  private:
    size_t sizeOfRecord{0};
    uint8_t* data{nullptr};
    size_t pos{0};
    size_t capacity{0};
    std::unique_ptr<BloomFilter> bloomFilter;
};

} // namespace NES::Runtime::Execution::Operators
#endif//NES_FIXEDPAGE_HPP
