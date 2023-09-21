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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGE_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGE_HPP_

#include <Runtime/BloomFilter.hpp>
#include <atomic>
#include <cstddef>
#include <memory>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Nautilus::Interface {
class FixedPageRef;
class PagedVectorRef;
class PagedVectorRefIter;

class FixedPage;
using FixedPagePtr = std::shared_ptr<FixedPage>;

/**
 * @brief class that stores the tuples on a page.
 * It also contains a bloom filter to have a quick check if a tuple is not on the page
 */
class FixedPage {
  public:
    static constexpr double BLOOM_FALSE_POSITIVE_RATE = 1e-2;
    static constexpr uint64_t PAGE_SIZE = 4096;

    /**
     * @brief Constructor for a FixedPage
     * @param dataPtr
     * @param sizeOfRecord
     * @param pageSize
     * @param bloomFalsePosRate
     */
    explicit FixedPage(int8_t* dataPtr,
                       size_t sizeOfRecord,
                       size_t pageSize = PAGE_SIZE,
                       double bloomFalsePosRate = BLOOM_FALSE_POSITIVE_RATE);

    /**
     * @brief Constructor for a FixedPage from another FixedPage
     * @param otherPage
     */
    FixedPage(FixedPage* otherPage);

    /**
     * @brief Constructor for a FixedPage from another FixedPage
     * @param otherPage
     */
    FixedPage(FixedPage&& otherPage);

    /**
     * @brief Constructor for a FixedPage from another FixedPage
     * @param otherPage
     */
    FixedPage& operator=(FixedPage&& otherPage);

    /**
     * @brief returns a pointer to the record at the given index
     * @param index
     * @return pointer to the record
     */
    int8_t* operator[](size_t index) const;

    /**
     * @brief returns a pointer to a memory location on this page where to write the record and checks if there is enough space for another record
     * @param hash
     * @return null pointer if there is no more space left on the page, otherwise the pointer
     */
    int8_t* append(const uint64_t hash);

    /**
     * @brief adds the hash to the BloomFilter
     * @param hash
     */
    void addHashToBloomFilter(const uint64_t hash);

    /**
     * @brief checks if the key might be in this page
     * @param hash
     * @return true or false
     */
    bool bloomFilterCheck(const uint64_t hash) const;

    /**
     * @brief returns the number of items on this page
     * @return no. items
     */
    size_t size() const;

    /**
     * @brief this methods returnds the content of the page as a string
     * @return string
     */
    std::string getContentAsString(SchemaPtr schema) const;

  private:
    friend FixedPageRef;
    friend PagedVectorRef;
    friend PagedVectorRefIter;

    /**
     * @brief Swapping lhs FixedPage with rhs FixedPage
     * @param lhs
     * @param rhs
     */
    void swap(FixedPage& lhs, FixedPage& rhs) noexcept;

    size_t sizeOfRecord;
    int8_t* data;
    //TODO check whether currentPos needs to be thread safe #4198
    std::atomic<size_t> currentPos;
    size_t capacity;
    std::unique_ptr<Runtime::BloomFilter> bloomFilter;
    double bloomFalsePosRate;
};

}// namespace NES::Nautilus::Interface
#endif// NES_RUNTIME_INCLUDE_NAUTILUS_INTERFACE_FIXEDPAGE_FIXEDPAGE_HPP_
