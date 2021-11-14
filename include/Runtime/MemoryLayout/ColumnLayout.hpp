/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_COLUMN_LAYOUT_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_COLUMN_LAYOUT_HPP_

#include <Runtime/MemoryLayout/MemoryLayout.hpp>

namespace NES::Runtime::MemoryLayouts {

class ColumnLayout;

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
class ColumnLayout : public MemoryLayout, public std::enable_shared_from_this<ColumnLayout> {
  public:
    /**
     * @brief Constructor to create a ColumnLayout according to a specific schema and a buffer size.
     * @param schema the underling schema of this memory layout.
     * @param bufferSize the expected buffer size.
     */
    ColumnLayout(SchemaPtr schema, uint64_t bufferSize);

    /**
     * @brief Factory to create a ColumnLayout
     * @param schema the underling schema of this memory layout.
     * @param bufferSize the expected buffer size.
     * @return ColumnLayoutPtr
     */
    static ColumnLayoutPtr create(SchemaPtr schema, uint64_t bufferSize);

    /**
     * @brief Calculates the offset in the tuple buffer of a particular field for a specific tuple.
     * For the column layout the field offset is calculated as follows:
     * \f$ offSet = (recordIndex * physicalFieldSizes[fieldIndex]) + columnOffsets[fieldIndex] \f$
     * @param tupleIndex index of the tuple.
     * @param fieldIndex index of the field.
     * @throws BufferAccessException if the tuple index or the field index is out of bounds.
     * @return offset in the tuple buffer.
     */
    [[nodiscard]] uint64_t getFieldOffset(uint64_t tupleIndex, uint64_t fieldIndex) const override;

    /**
     * This method binds a tuple buffer to this specific memory layout.
     * This enables to access the underling data very efficiently,
     * buf requires to fix the memory layout at compilation time.
     * If this is not practical consider to use the DynamicTupleBuffer.
     * @param tupleBuffer for that we want to bind the layout
     * @return shared_ptr to ColumnLayoutTupleBuffer
     */
    std::shared_ptr<ColumnLayoutTupleBuffer> bind(const TupleBuffer& tupleBuffer);

  private:
    std::vector<uint64_t> columnOffsets;
};

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_COLUMN_LAYOUT_HPP_
