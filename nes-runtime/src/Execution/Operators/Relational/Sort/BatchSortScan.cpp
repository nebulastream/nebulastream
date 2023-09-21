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

/**
 * The function RadixSortMSD is taken from DuckDB (MIT license):
 * MIT License
 *
 * Copyright 2018-2023 Stichting DuckDB Foundation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortOperatorHandler.hpp>
#include <Execution/Operators/Relational/Sort/BatchSortScan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <queue>

namespace NES::Runtime::Execution::Operators {
// We perform a radix sort on the byte level
// 256 is the number of possible byte values ranging from 0 to 255
constexpr uint64_t VALUES_PER_RADIX = 256;
// Size of locations mem area for MSD sort
constexpr uint64_t MSD_RADIX_LOCATIONS = VALUES_PER_RADIX + 1;
// Upper bound of number of records for which we use insertion sort
constexpr uint64_t INSERTION_SORT_THRESHOLD = 24;
// Lower bound of bytes to compare in mem region for which we use MSD sort
constexpr uint64_t MSD_RADIX_SORT_SIZE_THRESHOLD = 4;

/**
 * @brief Insertion sort implementation
 * It is efficient for small data sets and mostly sorted data.
 * @param orig_ptr pointer to original data
 * @param temp_ptr pointer to temporary data as working memory
 * @param count number of rows to sort
 * @param col_offset offset of column to sort
 * @param row_width width of row
 * @param total_comp_width total width of columns to compare
 * @param offset offset of columns to compare
 * @param swap swap original and temporary data
 */
inline void insertionSort(void* orig_ptr,
                          void* temp_ptr,
                          const uint64_t& count,
                          const uint64_t& col_offset,
                          const uint64_t& row_width,
                          const uint64_t& total_comp_width,
                          const uint64_t& offset,
                          bool swap) {
    auto* source_ptr = static_cast<uint8_t*>(swap ? temp_ptr : orig_ptr);
    auto* target_ptr = static_cast<uint8_t*>(swap ? orig_ptr : temp_ptr);
    if (count > 1) {
        const uint64_t total_offset = col_offset + offset;
        auto temp_val = std::unique_ptr<uint8_t[]>(new uint8_t[row_width]);
        uint8_t* val = temp_val.get();
        const auto comp_width = total_comp_width - offset;
        for (uint64_t i = 1; i < count; i++) {
            std::memcpy(val, source_ptr + i * row_width, row_width);
            uint64_t j = i;
            while (j > 0 && std::memcmp(source_ptr + (j - 1) * row_width + total_offset, val + total_offset, comp_width) > 0) {
                std::memcpy(source_ptr + j * row_width, source_ptr + (j - 1) * row_width, row_width);
                j--;
            }
            std::memcpy(source_ptr + j * row_width, val, row_width);
        }
    }
    if (swap) {
        memcpy(target_ptr, source_ptr, count * row_width);
    }
}

/**
 * @brief Radix sort implementation as least significant digit (lsd) radix sort
 * @param orig_ptr pointer to original data
 * @param temp_ptr pointer to temporary data as working memory
 * @param count number of rows to sort
 * @param col_offset offset of column to sort
 * @param row_width width of row
 * @param sorting_size number of bytes to sort
 */
void radixSortLSD(void* orig_ptr,
                  void* temp_ptr,
                  const uint64_t& count,
                  const uint64_t& col_offset,
                  const uint64_t& row_width,
                  const uint64_t& sorting_size) {
    bool swap = false;
    uint64_t counts[VALUES_PER_RADIX];
    for (uint64_t r = 1; r <= sorting_size; r++) {
        // Init counts to 0
        memset(counts, 0, sizeof(counts));
        // Const some values for convenience
        auto* source_ptr = static_cast<uint8_t*>(swap ? temp_ptr : orig_ptr);
        auto* target_ptr = static_cast<uint8_t*>(swap ? orig_ptr : temp_ptr);
        const uint64_t offset = col_offset + sorting_size - r;
        // Compute the prefix sum of the radix counts
        uint8_t* offset_ptr = source_ptr + offset;
        for (uint64_t i = 0; i < count; i++) {
            counts[*offset_ptr]++;
            offset_ptr += row_width;
        }
        uint64_t max_count = counts[0];
        for (uint64_t val = 1; val < VALUES_PER_RADIX; val++) {
            max_count = std::max(max_count, counts[val]);
            counts[val] = counts[val] + counts[val - 1];
        }
        if (max_count == count) {
            continue;
        }
        // Re-order the data in temporary array starting from the end
        uint8_t* row_ptr = source_ptr + (count - 1) * row_width;
        for (uint64_t i = 0; i < count; i++) {
            uint64_t& radix_offset = --counts[*(row_ptr + offset)];
            std::memcpy(target_ptr + radix_offset * row_width, row_ptr, row_width);
            row_ptr -= row_width;
        }
        swap = !swap;
    }
    // Move data back to original buffer (if it was swapped)
    if (swap) {
        memcpy(orig_ptr, temp_ptr, count * row_width);
    }
}

/**
 * @brief Radix sort implementation as most significant digit (msd) radix sort
 * @param orig_ptr pointer to original data
 * @param temp_ptr pointer to temporary data as working memory
 * @param count count of records
 * @param col_offset column offset for column to sort on
 * @param row_width size of a row
 * @param comp_width  size of the column
 * @param offset sort offset
 * @param locations locations array as working memory
 * @param swap swap temp and orig
 */
void radixSortMSD(void* orig_ptr,
                  void* temp_ptr,
                  const uint64_t& count,
                  const uint64_t& col_offset,
                  const uint64_t& row_width,
                  const uint64_t& comp_width,
                  const uint64_t& offset,
                  uint64_t locations[],
                  bool swap) {
    // Set source and target pointers based on the swap flag
    auto* source_ptr = static_cast<uint8_t*>(swap ? temp_ptr : orig_ptr);
    auto* target_ptr = static_cast<uint8_t*>(swap ? orig_ptr : temp_ptr);

    // Initialize locations array to zero
    memset(locations, 0, MSD_RADIX_LOCATIONS * sizeof(uint64_t));

    // Set the counts pointer to the next position of the locations array
    uint64_t* counts = locations + 1;

    // Calculate the total offset as the sum of column offset and provided offset
    const uint64_t total_offset = col_offset + offset;

    // Set the offset_ptr to point at the source data plus the total_offset
    auto* offset_ptr = const_cast<uint8_t*>(source_ptr + total_offset);

    // Loop through rows and collect counts of byte values at the specified offset
    for (uint64_t i = 0; i < count; i++) {
        counts[*offset_ptr]++;
        offset_ptr += row_width;
    }

    // Initialize a variable to store the maximum count of any byte value
    uint64_t max_count = 0;

    // Loop through all possible byte values (radix) and update the maximum count and locations array
    for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        max_count = std::max(max_count, counts[radix]);
        counts[radix] += locations[radix];
    }

    // If maximum count is not equal to the total count, reorder the rows based on the calculated locations
    if (max_count != count) {
        const uint8_t* row_ptr = source_ptr;
        for (uint64_t i = 0; i < count; i++) {
            const uint64_t& radix_offset = locations[*(row_ptr + total_offset)]++;
            std::memcpy((void*) (target_ptr + radix_offset * row_width), row_ptr, row_width);
            row_ptr += row_width;
        }
        // Toggle the swap flag after reordering
        swap = !swap;
    }

    // If the current offset is the last one in the comparison width, check if swap flag is true and copy data back to original data
    if (offset == comp_width - 1) {
        if (swap) {
            std::memcpy((void*) orig_ptr, temp_ptr, count * row_width);
        }
        return;
    }

    // If the maximum count is equal to the total count, call RadixSortMSD recursively with an increased offset
    if (max_count == count) {
        radixSortMSD(orig_ptr,
                     temp_ptr,
                     count,
                     col_offset,
                     row_width,
                     comp_width,
                     offset + 1,
                     locations + MSD_RADIX_LOCATIONS,
                     swap);
        return;
    }

    // If the function hasn't returned yet, call RadixSortMSD recursively for each byte value (radix) with the corresponding count and locations
    uint64_t radix_count = locations[0];
    for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        const uint64_t loc = (locations[radix] - radix_count) * row_width;
        if (radix_count > INSERTION_SORT_THRESHOLD) {
            radixSortMSD(static_cast<uint8_t*>(orig_ptr) + loc,
                         static_cast<uint8_t*>(temp_ptr) + loc,
                         radix_count,
                         col_offset,
                         row_width,
                         comp_width,
                         offset + 1,
                         locations + MSD_RADIX_LOCATIONS,
                         swap);
        } else if (radix_count != 0) {
            // When the count is low (less than the threshold), insertion sort is more efficient
            insertionSort(static_cast<uint8_t*>(orig_ptr) + loc,
                          static_cast<uint8_t*>(temp_ptr) + loc,
                          radix_count,
                          col_offset,
                          row_width,
                          comp_width,
                          offset + 1,
                          swap);
        }
        radix_count = locations[radix + 1] - locations[radix];
    }
}

/**
 * @brief Simple implementation of a K-way merge using std::priority_queue
 * Future implementations should consider a cascade merge sort
 * @brief origin pointer to original data
 * @brief tmp pointer to temporary data
 * @brief k number of pages to merge
 * @brief compWidth size of the column
 * @brief rowWidth size of a row
 */
void kWayMerge(Nautilus::Interface::PagedVector* origin,
               Nautilus::Interface::PagedVector* tmp,
               uint32_t k,
               uint32_t compWidth,
               uint32_t rowWidth) {
    // Structure to represent an element with its value and index
    struct Element {
        int8_t* value;
        uint32_t arrayIndex;
        uint32_t elementIndex;
        Element(int8_t* val, uint32_t arrIndex, uint32_t elemIndex) : value(val), arrayIndex(arrIndex), elementIndex(elemIndex) {}
    };

    // Custom comparator for the priority queue
    struct ElementComparator {
        uint32_t compWidth;
        explicit ElementComparator(uint32_t width) { compWidth = width; }
        bool operator()(Element a, Element b) const { return std::memcmp(a.value, b.value, compWidth) > 0; }
    };

    ElementComparator comp(compWidth);
    std::priority_queue<Element, std::vector<Element>, ElementComparator> heap(comp);

    // Initialize the heap with the first element from each page
    for (uint32_t i = 0; i < k; ++i) {
        // We assume here that the pages cannot be empty
        heap.emplace((*origin->getPages()[i])[0], i, 0);
    }

    auto resultCnt = 0;
    // Merge the pages until the heap is empty
    while (!heap.empty()) {
        // Get the max element from the heap
        Element maxElement = heap.top();
        heap.pop();

        uint32_t arrayIndex = maxElement.arrayIndex;
        uint32_t elementIndex = maxElement.elementIndex;

        // Add the max element to the merged array
        if (resultCnt++ % tmp->getCapacityPerPage() == 0) {
            tmp->appendPage();
        }
        std::memcpy(tmp->getEntry(resultCnt), maxElement.value, rowWidth);

        // Move to the next element in the array
        // Last page
        if (arrayIndex + 1 == origin->getNumberOfPages()) {
            if (elementIndex + 1 < origin->getNumberOfEntriesOnCurrentPage()) {
                heap.emplace((*origin->getPages()[arrayIndex])[0] + (rowWidth * (elementIndex + 1)), arrayIndex, elementIndex + 1);
            }
        } else {// other pages
            if (elementIndex + 1 < origin->getCapacityPerPage()) {
                heap.emplace((*origin->getPages()[arrayIndex])[0] + (rowWidth * (elementIndex + 1)), arrayIndex, elementIndex + 1);
            }
        }
    }
}

void SortProxy(void* op, uint64_t compWidth, uint64_t colOffset) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    auto rowWidth = handler->getStateEntrySize();

    for (uint32_t i = 0; i < handler->getState()->getNumberOfPages(); ++i) {
        auto origPtr = (*handler->getState()->getPages()[i])[0];
        // append page if not existing
        if (handler->getTempState()->getPages().size() <= i) {
            handler->getTempState()->appendPage();
        }
        auto tempPtr = (*handler->getTempState()->getPages()[i])[0];

        auto count = handler->getState()->getCapacityPerPage();
        if (i + 1 == handler->getState()->getNumberOfPages()) {
            count = handler->getState()->getNumberOfEntriesOnCurrentPage();
        }
        auto offset = 0;  // init to 0
        auto swap = false;// init false

        if (count <= INSERTION_SORT_THRESHOLD) {
            insertionSort(origPtr, tempPtr, count, colOffset, rowWidth, compWidth, offset, swap);
        } else if (compWidth <= MSD_RADIX_SORT_SIZE_THRESHOLD) {
            radixSortLSD(origPtr, tempPtr, count, colOffset, rowWidth, compWidth);
        } else {
            auto locations = new uint64_t[compWidth * MSD_RADIX_LOCATIONS];
            radixSortMSD(origPtr, tempPtr, count, colOffset, rowWidth, compWidth, offset, locations, swap);
            delete[] locations;
        }
    }

    // Merge
    kWayMerge(handler->getState(), handler->getTempState(), handler->getState()->getNumberOfPages(), compWidth, rowWidth);
}

void* getStateProxy(void* op) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    // return here the temp state holding the merged fields
    handler->getTempState()->setTotalNumberOfEntries(handler->getState()->getTotalNumberOfEntries());
    return handler->getTempState();
}

uint64_t getSortStateEntrySizeProxy(void* op) {
    auto handler = static_cast<BatchSortOperatorHandler*>(op);
    return handler->getStateEntrySize();
}

void BatchSortScan::setup(ExecutionContext& ctx) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // Set the width of the current column to sort
    uint64_t compWidth = 0;
    for (const auto& sortFieldIdentifier : sortFieldIdentifiers) {
        for (uint64_t i = 0; i < fieldIdentifiers.size(); ++i) {
            if (fieldIdentifiers[i] == sortFieldIdentifier) {
                compWidth += dataTypes[i]->size();
            }
        }
    }
    // Offset of the current column to sort: For now this is always 0
    constexpr uint64_t colOffset = 0;

    // perform sort
    Nautilus::FunctionCall("SortProxy", SortProxy, globalOperatorHandler, Value<UInt64>(compWidth), Value<UInt64>(colOffset));
}

void BatchSortScan::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    Operators::Operator::open(ctx, rb);

    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // 2. load the state
    auto stateProxy = Nautilus::FunctionCall("getStateProxy", getStateProxy, globalOperatorHandler);
    auto entrySize = Nautilus::FunctionCall("getSortStateEntrySizeProxy", getSortStateEntrySizeProxy, globalOperatorHandler);
    auto state = Nautilus::Interface::PagedVectorRef(stateProxy, entrySize->getValue());

    // 3. emit the records
    // We start here with index 1. We may want to investigate why the content of the paged vector is shifted by 1
    for (uint64_t entryIndex = 1; entryIndex < state.getTotalNumberOfEntries() + 1; entryIndex++) {
        Record record;
        auto entry = state.getEntry(Value<UInt64>(entryIndex));
        // skip encoded data
        for (uint64_t i = 0; i < fieldIdentifiers.size(); ++i) {
            for (uint64_t j = 0; j < sortFieldIdentifiers.size(); ++j) {
                if (fieldIdentifiers[i] == sortFieldIdentifiers[j]) {
                    entry = entry + (uint64_t) dataTypes[i]->size();
                }
            }
        }
        for (uint64_t i = 0; i < fieldIdentifiers.size(); ++i) {
            auto value = MemRefUtils::loadValue(entry, dataTypes[i]);
            record.write(fieldIdentifiers[i], value);
            entry = entry + (uint64_t) dataTypes[i]->size();
        }
        child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators