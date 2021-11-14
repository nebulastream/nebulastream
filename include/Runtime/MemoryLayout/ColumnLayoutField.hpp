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

#ifndef NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_FIELD_HPP_
#define NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_FIELD_HPP_

#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES::Runtime::MemoryLayouts {

/**
 * @brief The ColumnLayoutField enables assesses to a specific field in a columnar layout.
 * It overrides the operator[] for a more user friendly access of tuples for a predefined field.
 * As this required direct knowledge of a particular memory layout at compile-time, consider to use the DynamicTupleBuffer.
 * @tparam T the type of the field
 * @tparam boundaryChecks flag to identify if buffer bounds should be checked at runtime.
 * @caution This class is non-thread safe
 */
template<class T, bool boundaryChecks = true>
class ColumnLayoutField {

  public:
    /**
     * Factory to create a ColumnLayoutField for a specific memory layout and a specific tuple buffer.
     * @param fieldIndex the field which is accessed.
     * @param layout the memory layout
     * @tparam buffer the tuple buffer
     * @tparam T type of field
     * @return field handler
     */
    static inline ColumnLayoutField<T, boundaryChecks> create(uint64_t fieldIndex, std::shared_ptr<ColumnLayout> layout, TupleBuffer& buffer);

    /**
     * Creates a ColumnLayoutField for a specific memory layout and a specific tuple buffer.
     * @param fieldIndex
     * @param layout the memory layout
     * @tparam buffer the tuple buffer
     * @tparam T type of field
     * @return field handler via a fieldName and a layoutBuffer
     */
    static inline ColumnLayoutField<T, boundaryChecks>
    create(const std::string& fieldName, std::shared_ptr<ColumnLayout> layout, TupleBuffer& buffer);

    /**
     * Accesses the value of this field for a specific record.
     * @param recordIndex
     * @return reference to a field attribute from the created field handler accessed by recordIndex
     */
    inline T& operator[](size_t recordIndex);

  private:
    /**
     * @brief Constructor for ColumnLayoutField
     * @param basePointer
     * @param layout
     */
    ColumnLayoutField(T* basePointer, std::shared_ptr<ColumnLayout> layout) : basePointer(basePointer), layout(std::move(layout)){};

    T* basePointer;
    std::shared_ptr<ColumnLayout> layout;
};

template<class T, bool boundaryChecks>
inline ColumnLayoutField<T, boundaryChecks>
ColumnLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, std::shared_ptr<ColumnLayout> layout, TupleBuffer& buffer) {
    if (boundaryChecks && fieldIndex >= layout->getFieldSizes().size()) {
        NES_THROW_RUNTIME_ERROR("fieldIndex out of bounds! " << layout->getFieldSizes().size() << " >= " << fieldIndex);
    }

    auto* bufferBasePointer = &(buffer.getBuffer<uint8_t>()[0]);
    auto fieldOffset = layout->getFieldOffset(0, fieldIndex);

    T* basePointer = reinterpret_cast<T*>(bufferBasePointer + fieldOffset);
    return ColumnLayoutField<T, boundaryChecks>(basePointer, layout);
}

template<class T, bool boundaryChecks>
ColumnLayoutField<T, boundaryChecks>
ColumnLayoutField<T, boundaryChecks>::create(const std::string& fieldName, std::shared_ptr<ColumnLayout> layout, TupleBuffer& buffer) {
    auto fieldIndex = layout->getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value()) {
        return ColumnLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layout, buffer);
    }
    NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutField: Could not find fieldIndex for " << fieldName);
}

template<class T, bool boundaryChecks>
inline T& ColumnLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks && recordIndex >= layout->getCapacity()) {
        NES_THROW_RUNTIME_ERROR("recordIndex out of bounds!" << layout->getCapacity()
                                                             << " >= " << recordIndex);
    }
    return *(basePointer + recordIndex);
}

}// namespace NES::Runtime::MemoryLayouts

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_FIELD_HPP_
