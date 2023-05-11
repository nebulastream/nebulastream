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

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Relational/Sort.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/Stack/Stack.hpp>
#include <Nautilus/Interface/Stack/StackRef.hpp>
#include <cstring>

namespace NES::Runtime::Execution::Operators {

const uint64_t VALUES_PER_RADIX = 256;
const uint64_t MSD_RADIX_LOCATIONS = VALUES_PER_RADIX + 1;

template <typename T>
T MaxValue(T a, T b) {
    return a > b ? a : b;
}

/**
 * @brief Radix sort implementation as MSD radix sort
 * Code take from DuckDB:
 *  https://github.com/duckdb/duckdb/blob/42ea342a29e802b2a8e7e71b88b5bc0c3a029279/src/common/sort/radix_sort.cpp#L239
 * @param orig_ptr -> data pointer
 * @param temp_ptr -> temporary data pointer -> buffer_manager.Allocate(MaxValue(count * sort_layout.entry_size, (idx_t)Storage::BLOCK_SIZE));
 * @param count -> count of records
 * @param col_offset -> column offset
 * @param row_width -> x -> entry size
 * @param comp_width sorting_size
 * @param offset  -> init 0
 * @param locations -> unique_ptr<idx_t[]>(new idx_t[sorting_size * MSD_RADIX_LOCATIONS])
 * @param swap -> false
 */
void RadixSortMSD(const uint8_t *orig_ptr, const uint8_t *temp_ptr, const uint64_t &count, const uint64_t &col_offset,
                  const uint64_t &row_width, const uint64_t &comp_width, const uint64_t &offset, uint64_t locations[], bool swap) {

    // Set source and target pointers based on the swap flag
    const uint8_t *source_ptr = swap ? temp_ptr : orig_ptr;
    const uint8_t *target_ptr = swap ? orig_ptr : temp_ptr;

    // Initialize locations array to zero
    memset(locations, 0, MSD_RADIX_LOCATIONS * sizeof(uint64_t));

    // Set the counts pointer to the next position of the locations array
    uint64_t *counts = locations + 1;

    // Calculate the total offset as the sum of column offset and provided offset
    const uint64_t total_offset = col_offset + offset;

    // Set the offset_ptr to point at the source data plus the total_offset
    auto *offset_ptr = const_cast<uint8_t*>(source_ptr + total_offset);

    // Loop through rows and collect counts of byte values at the specified offset
    for (uint64_t i = 0; i < count; i++) {
        counts[*offset_ptr]++;
        offset_ptr += row_width;
    }

    // Initialize a variable to store the maximum count of any byte value
    uint64_t max_count = 0;

    // Loop through all possible byte values (radix) and update the maximum count and locations array
    for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        max_count = MaxValue<uint64_t>(max_count, counts[radix]);
        counts[radix] += locations[radix];
    }

    // If maximum count is not equal to the total count, reorder the rows based on the calculated locations
    if (max_count != count) {
        const uint8_t *row_ptr = source_ptr;
        for (uint64_t i = 0; i < count; i++) {
            const uint64_t &radix_offset = locations[*(row_ptr + total_offset)]++;
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
        RadixSortMSD(orig_ptr, temp_ptr, count, col_offset, row_width, comp_width, offset + 1,
                     locations + MSD_RADIX_LOCATIONS, swap);
        return;
    }

    // If the function hasn't returned yet, call RadixSortMSD recursively for each byte value (radix) with the corresponding count and locations
    uint64_t radix_count = locations[0];
    for (uint64_t radix = 0; radix < VALUES_PER_RADIX; radix++) {
        const uint64_t loc = (locations[radix] - radix_count) * row_width;
        RadixSortMSD(orig_ptr + loc, temp_ptr + loc, radix_count, col_offset, row_width, comp_width, offset + 1,
                         locations + MSD_RADIX_LOCATIONS, swap);
        radix_count = locations[radix + 1] - locations[radix];
    }
}

class LocalSortState : public Operators::OperatorState {
  public:
    explicit LocalSortState(Interface::StackRef stack) : stack(std::move(stack)){};

    Interface::StackRef stack;
};

Sort::Sort(const uint64_t operatorHandlerIndex, const std::vector<Expressions::ExpressionPtr>& sortExpressions, const std::vector<PhysicalTypePtr>& sortDataTypes)
    : operatorHandlerIndex(operatorHandlerIndex), sortExpressions(sortExpressions), sortDataTypes(sortDataTypes) {

    for (auto& sortType : sortDataTypes) {
        keySize = keySize + sortType->size();
    }
}

void Sort::execute(ExecutionContext& ctx, Record& record) const {
    // Derive the column to sort on
    std::vector<Value<>> sortValues;
    for (const auto& exp : sortExpressions) {
        sortValues.emplace_back(exp->execute(record));
    }

    // Store all the tuples into a thread local buffer
    auto state = reinterpret_cast<LocalSortState*>(ctx.getLocalState(this));
    auto& stack = state->stack;

    // create entry and store it in stack
    auto entry = stack.allocateEntry();
    /* 4.3a store hash value at next offset
    auto hashPtr = (entry + (uint64_t) sizeof(int64_t)).as<MemRef>();
    hashPtr.store(hash);

    // 4.3b store key values
    auto keyPtr = (hashPtr + (uint64_t) sizeof(int64_t)).as<MemRef>();
    storeKeys(keyValues, keyPtr);

    // 4.3c store value values
    std::vector<Value<>> values;
    for (const auto& exp : valueExpressions) {
        values.emplace_back(exp->execute(record));
    }
    auto valuePtr = (keyPtr + keySize).as<MemRef>();
    storeValues(values, valuePtr);*/


    // Radix Sort
    // Byte-by-byte sorting

    // 1. derive sort values
    /*std::vector<Value<>> SortValues;
    for (const auto& exp : sortExpressions) {
        sortValues.emplace_back(exp->execute(record));
    }*/

    // 2. Store them on the

    // TODO: support multi column sorting
    // TODO: support descending order
    // TODO: support column oriented record layouts
    // TODO: depending on the data characteristics, we may want to use a different sorting algorithm

    // Morsel Driven Parallelism
    // 1. Split data into morsels
    // 2. Sort morsels in parallel
    // 3. Merge morsels
    // 4. Repeat 2-3 until only one morsel is left


    // K-way merge

    // evaluate expression and call child operator if expression is valid
    /*if (expression->execute(record)) {
        if (child != nullptr) {
            child->execute(ctx, record);
        }
    }*/
}

// This will be called when the tuple was fully processed
void Sort::close(ExecutionContext& executionCtx,  RecordBuffer& recordBuffer) const {
    // Perform the actual sort
    //RadixSortMSD(..)
}

}// namespace NES::Runtime::Execution::Operators