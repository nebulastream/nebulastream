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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_FIELD_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_FIELD_HPP_

#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <utility>

namespace NES::Runtime::MemoryLayouts {

/**
 * @brief This class is used for handling fields in a given DynamicColumnLayoutBuffer. It also overrides the operator[] for a more user friendly access of records for a predefined field.
 * @tparam T
 * @tparam boundaryChecks
 * @caution This class is non-thread safe
 */
template<class T, bool boundaryChecks>
class RowLayoutField {

  public:
    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldIndex and a layoutBuffer
     */
    static inline RowLayoutField<T, boundaryChecks>
    create(uint64_t fieldIndex, std::shared_ptr<RowLayout> layout, TupleBuffer& buffer);

    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldName and a layoutBuffer
     */
    static inline RowLayoutField<T, boundaryChecks>
    create(const std::string& fieldName, std::shared_ptr<RowLayout> layout, TupleBuffer& buffer);

    /**
     * @param recordIndex
     * @return reference to a field attribute from the created field handler accessed by recordIndex
     */
    inline T& operator[](size_t recordIndex);

  private:
    /**
     * @brief Constructor for DynamicRowLayoutField
     * @param dynamicRowLayoutBuffer
     * @param basePointer
     * @param fieldIndex
     * @param recordSize
     */
    RowLayoutField(std::shared_ptr<RowLayout> layout, uint8_t* basePointer, FIELD_SIZE fieldIndex, FIELD_SIZE recordSize)
        : fieldIndex(fieldIndex), recordSize(recordSize), basePointer(basePointer), layout(std::move(layout)){};

    const FIELD_SIZE fieldIndex;
    const FIELD_SIZE recordSize;
    uint8_t* basePointer;
    std::shared_ptr<RowLayout> layout;
};

template<class T, bool boundaryChecks>
inline RowLayoutField<T, boundaryChecks>
RowLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, std::shared_ptr<RowLayout> layout, TupleBuffer& buffer) {
    if (boundaryChecks && fieldIndex >= layout->getFieldOffSets().size()) {
        NES_THROW_RUNTIME_ERROR("fieldIndex out of bounds!" << layout->getFieldOffSets().size() << " >= " << fieldIndex);
    }

    // via pointer arithmetic gets the starting field address
    auto* bufferBasePointer = &(buffer.getBuffer<uint8_t>()[0]);
    auto offSet = layout->getFieldOffset(0, fieldIndex);
    auto* basePointer = bufferBasePointer + offSet;

    return RowLayoutField<T, boundaryChecks>(layout, basePointer, fieldIndex, layout->getRecordSize());
}

template<class T, bool boundaryChecks>
inline RowLayoutField<T, boundaryChecks>
RowLayoutField<T, boundaryChecks>::create(const std::string& fieldName, std::shared_ptr<RowLayout> layout, TupleBuffer& buffer) {
    auto fieldIndex = layout->getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value()) {
        return RowLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layout, buffer);
    }
    NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutField: Could not find fieldIndex for " << fieldName);
}

template<class T, bool boundaryChecks>
inline T& RowLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks && recordIndex >= layout->getCapacity()) {
        NES_THROW_RUNTIME_ERROR("recordIndex out of bounds!" << layout->getCapacity() << " >= " << recordIndex);
    }

    return *reinterpret_cast<T*>(basePointer + recordSize * recordIndex);
}

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_ROW_LAYOUT_FIELD_HPP_
