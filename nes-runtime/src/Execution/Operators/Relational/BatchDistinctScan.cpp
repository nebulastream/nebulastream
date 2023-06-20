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

#include <Execution/Operators/Relational/BatchDistinctOperatorHandler.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Relational/BatchDistinctScan.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <cstdint>
#include <cstdlib>
#include <unordered_set>

namespace NES::Runtime::Execution::Operators {

// TODO: Implement Distinct Scan on a single column (and single page of PageVector)
// const uint64_t VALUES_PER_RADIX = 256;
// const uint64_t MSD_RADIX_LOCATIONS = VALUES_PER_RADIX + 1;
// const uint64_t COL_SIZE = 8;

// /**
//  * @brief Radix sort implementation as most significant digit (msd) radix sort
//  * @param orig_ptr pointer to original data
//  * @param temp_ptr pointer to temporary data as working memory
//  * @param count count of records
//  * @param col_offset column offset for column to sort on
//  * @param row_width size of a row
//  * @param comp_width  size of the column
//  * @param offset sort offset
//  * @param locations locations array as working memory
//  * @param swap swap temp and orig
//  */
// void RadixSortMSD(void* orig_ptr,
//                   void* temp_ptr,
//                   const uint64_t& count,
//                   const uint64_t& col_offset,
//                   const uint64_t& row_width,
//                   const uint64_t& comp_width,
//                   const uint64_t& offset,
//                   uint64_t locations[],
//                   bool swap) {
//     // Set source and target pointers based on the swap flag
//     uint8_t* source_ptr = static_cast<uint8_t*>(swap ? temp_ptr : orig_ptr);
//     uint8_t* target_ptr = static_cast<uint8_t*>(swap ? orig_ptr : temp_ptr);

//     // Initialize locations array to zero
//     memset(locations, 0, MSD_RADIX_LOCATIONS * sizeof(uint64_t));

//     // Set the counts pointer to the next position of the locations array
//     uint64_t* counts = locations + 1;

//     // Calculate the total offset as the sum of column offset and provided offset
//     const uint64_t total_offset = col_offset + offset;

//     // Set the offset_ptr to point at the source data plus the total_offset
//     auto* offset_ptr = const_cast<uint8_t*>(source_ptr + total_offset);

//     // Loop through rows and collect counts of byte values at the specified offset
//     for (uint64_t i = 0; i < count; i++) {
//         counts[*offset_ptr]++;
//         offset_ptr += row_width;
//     }

//     // Initialize a variable to store the maximum count of any byte value
//     uint64_t max_count = 0;

//     // Loop through all possible byte values (radix) and update the maximum count and locations array
//     for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
//         max_count = std::max(max_count, counts[radix]);
//         counts[radix] += locations[radix];
//     }

//     // If maximum count is not equal to the total count, reorder the rows based on the calculated locations
//     if (max_count != count) {
//         const uint8_t* row_ptr = source_ptr;
//         for (uint64_t i = 0; i < count; i++) {
//             const uint64_t& radix_offset = locations[*(row_ptr + total_offset)]++;
//             std::memcpy((void*) (target_ptr + radix_offset * row_width), row_ptr, row_width);
//             row_ptr += row_width;
//         }
//         // Toggle the swap flag after reordering
//         swap = !swap;
//     }

//     // If the current offset is the last one in the comparison width, check if swap flag is true and copy data back to original data
//     if (offset == comp_width - 1) {
//         if (swap) {
//             std::memcpy((void*) orig_ptr, temp_ptr, count * row_width);
//         }
//         return;
//     }

//     // If the maximum count is equal to the total count, call RadixSortMSD recursively with an increased offset
//     if (max_count == count) {
//         RadixSortMSD(orig_ptr,
//                      temp_ptr,
//                      count,
//                      col_offset,
//                      row_width,
//                      comp_width,
//                      offset + 1,
//                      locations + MSD_RADIX_LOCATIONS,
//                      swap);
//         return;
//     }

//     // If the function hasn't returned yet, call RadixSortMSD recursively for each byte value (radix) with the corresponding count and locations
//     uint64_t radix_count = locations[0];
//     for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
//         const uint64_t loc = (locations[radix] - radix_count) * row_width;
//         RadixSortMSD(static_cast<uint8_t*>(orig_ptr) + loc,
//                      static_cast<uint8_t*>(temp_ptr) + loc,
//                      radix_count,
//                      col_offset,
//                      row_width,
//                      comp_width,
//                      offset + 1,
//                      locations + MSD_RADIX_LOCATIONS,
//                      swap);
//         radix_count = locations[radix + 1] - locations[radix];
//     }
// }

// void RadixSortMSDProxy(void* op) {
//     auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
//     // TODO issue #3773 add support for data larger than page size
//     auto origPtr = handler->getState()->getEntry(0);
//     auto tempPtr = handler->getTempState()->getEntry(0);
//     auto count = handler->getState()->getNumberOfEntries();
//     // TODO issue #3773 add support for columns other than the first one
//     auto colOffset = 0;
//     auto rowWidth = handler->getStateEntrySize();
//     // TODO issue #3773 add support for columns other sizes
//     auto compWidth = COL_SIZE;
//     auto offset = 0;// init to 0
//     auto locations = new uint64_t[compWidth * MSD_RADIX_LOCATIONS];
//     auto swap = false;// init false
//     RadixSortMSD(origPtr, tempPtr, count, colOffset, rowWidth, compWidth, offset, locations, swap);
//     delete[] locations;
// }

void applyDistinctOperator() {
    // Todo build hash set, then return keys

}

void distinctProxy(void* op) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    // TODO add support for data larger than page size
    auto origPtr = handler->getState()->getEntry(0);
    auto tempPtr = handler->getTempState()->getEntry(0);
    auto count = handler->getState()->getNumberOfEntries();
    // TODO add support for columns other than the first one
    // auto colOffset = 0;
    // auto rowWidth = handler->getStateEntrySize();
    // auto offset = 0;// init to 0
    //Todo loop over entries in state and build hash set
    std::unordered_set<uint64_t> distinctValues;
    for(size_t i = 0; i < handler->getState()->getNumberOfEntries(); ++i) {
        auto entry = reinterpret_cast<uint32_t*>(handler->getTempState()->getEntry(i));
        distinctValues.emplace(*entry);
        // free(entry);
        // free(entry2);
    }
    // hash set needs to be transferred to state!
    // Todo: transfer hashset to state

    //Here the main work is executed
    // applyDistinctOperator();
}

void* getStateProxyDistinct(void* op) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    return handler->getState();
}

uint64_t getSortStateEntrySizeProxyDistinct(void* op) {
    auto handler = static_cast<BatchDistinctOperatorHandler*>(op);
    return handler->getStateEntrySize();
}

void BatchDistinctScan::setup(ExecutionContext& ctx) const {
    // perform sort
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    //Todo perform function call
    Nautilus::FunctionCall("distinctProxy", distinctProxy, globalOperatorHandler);
}

void BatchDistinctScan::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    Operators::Operator::open(ctx, rb);

    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // 2. load the state
    auto stateProxy = Nautilus::FunctionCall("getStateProxy", getStateProxyDistinct, globalOperatorHandler);
    auto entrySize = Nautilus::FunctionCall("getSortStateEntrySizeProxy", getSortStateEntrySizeProxyDistinct, globalOperatorHandler);
    auto state = Nautilus::Interface::PagedVectorRef(stateProxy, entrySize->getValue());

    // 3. emit the records
    for (uint64_t entryIndex = 0; entryIndex < state.getNumberOfEntries(); entryIndex++) {
        Record record;
        auto entry = state.getEntry(Value<UInt64>(entryIndex));
        for (uint64_t i = 0; i < fieldIdentifiers.size(); i++) {
            auto value = MemRefUtils::loadValue(entry, dataTypes[i]);
            record.write(fieldIdentifiers[i], value);
            entry = entry + dataTypes[i]->size();
        }
        child->execute(ctx, record);
    }
}

}// namespace NES::Runtime::Execution::Operators