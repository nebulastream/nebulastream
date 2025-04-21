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

#pragma once

#include <cstdint>
#include <memory>
#include <API/Schema.hpp>
#include <MemoryLayout/MemoryLayout.hpp>

namespace NES::Memory::MemoryLayouts
{

/**
 * @brief Implements a columnar layout, that maps all tuples in a tuple buffer to a column-wise layout.
 * For a schema with 3 fields (F1, F2, and F3) we retrieve the following layout.
 *
 * | F1, F1, F1 |
 * | F2, F2, F2 |
 * | F3, F3, F3 |
 *
 * This may be beneficial for processing performance if only a subset fields of the tuple are accessed.
 */
class ColumnLayout : public MemoryLayout
{
public:
    ColumnLayout(const Schema& schema, uint64_t bufferSize);
    ColumnLayout(const ColumnLayout& other);

    /// @brief Calculates the offset in the tuple buffer of a particular field for a specific tuple.
    /// For the column layout the field offset is calculated as follows:
    /// \f$ offSet = (recordIndex * physicalFieldSizes[fieldIndex]) + columnOffsets[fieldIndex] \f$
    /// @throws CannotAccessBuffer if the tuple index or the field index is out of bounds.
    /// @return offset in the tuple buffer.
    [[nodiscard]] uint64_t getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const override;

    [[nodiscard]] uint64_t getColumnOffset(uint64_t fieldIndex) const;

private:
    std::vector<uint64_t> columnOffsets;
};

}
