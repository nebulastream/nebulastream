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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_FIXEDPAGE_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_FIXEDPAGE_HPP_

#include <Runtime/BloomFilter.hpp>
#include <atomic>
#include <cstddef>
#include <memory>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
}// namespace NES

namespace NES::Runtime::Execution::Operators {

/**
 * @brief class that stores the tuples on a page.
 * It also contains a bloom filter to have a quick check if a tuple is not on the page
 */
class FixedPage {
  public:
    /**
     * @brief Constructor for a FixedPage
     * @param tail
     * @param overrunAddress
     * @param sizeOfRecord
     * @param pageSize
     */
    explicit FixedPage(uint8_t* dataPtr, size_t sizeOfRecord, size_t pageSize);

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
    uint8_t* operator[](size_t index) const;

    /**
     * @brief returns a pointer to the record at the given index
     * @param index
     * @return pointer to the record
     */
    uint8_t* getRecord(size_t index) const;

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

    /**
     * @brief this methods tests if requiredSpace is left on page
     * @return bool
     */
    bool isSizeLeft(uint64_t requiredSpace) const;

    /**
     * @brief this methods returnds the content of the page as a string
     * @return string
     */
    std::string getContentAsString(SchemaPtr schema) const;

  private:
    /**
     * @brief Swapping lhs FixedPage with rhs FixedPage
     * @param lhs
     * @param rhs
     */
    void swap(FixedPage& lhs, FixedPage& rhs) noexcept;

  private:
    size_t sizeOfRecord;
    uint8_t* data;
    std::atomic<size_t> currentPos;
    size_t capacity;
    std::unique_ptr<BloomFilter> bloomFilter;
};

}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMHASHJOIN_DATASTRUCTURE_FIXEDPAGE_HPP_
