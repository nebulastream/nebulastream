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

#include <Runtime/MemoryLayout/DynamicColumnLayoutBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicMemoryLayout.hpp>
#include <Runtime/NodeEngineForwaredRefs.hpp>

namespace NES::Runtime::DynamicMemoryLayout {

/**
 * @brief This class is used for handling fields in a given DynamicColumnLayoutBuffer. It also overrides the operator[] for a more user friendly access of records for a predefined field.
 * @tparam T
 * @tparam boundaryChecks
 * @caution This class is non-thread safe
 */
template<class T, bool boundaryChecks>
class DynamicColumnLayoutField {

  public:
    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldIndex and a layoutBuffer
     */
    static inline DynamicColumnLayoutField<T, boundaryChecks> create(uint64_t fieldIndex,
                                                                     DynamicColumnLayoutBufferPtr layoutBuffer);

    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldName and a layoutBuffer
     */
    static inline DynamicColumnLayoutField<T, boundaryChecks> create(const std::string& fieldName,
                                                                     DynamicColumnLayoutBufferPtr layoutBuffer);

    /**
     * @param recordIndex
     * @return reference to a field attribute from the created field handler accessed by recordIndex
     */
    inline T& operator[](size_t recordIndex);

  private:
    /**
     * @brief Constructor for DynamicColumnLayoutField
     * @param basePointer
     * @param dynamicColumnLayoutBuffer
     */
    DynamicColumnLayoutField(T* basePointer, DynamicColumnLayoutBufferPtr dynamicColumnLayoutBuffer)
        : basePointer(basePointer), dynamicColumnLayoutBuffer(std::move(dynamicColumnLayoutBuffer)){};

    T* basePointer;
    DynamicColumnLayoutBufferPtr dynamicColumnLayoutBuffer;
};

template<class T, bool boundaryChecks>
inline DynamicColumnLayoutField<T, boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex,
                                                    std::shared_ptr<DynamicColumnLayoutBuffer> layoutBuffer) {
    if (boundaryChecks && fieldIndex >= layoutBuffer->getFieldSizes().size()) {
        NES_THROW_RUNTIME_ERROR("fieldIndex out of bounds! " << layoutBuffer->getFieldSizes().size() << " >= " << fieldIndex);
    }

    auto* bufferBasePointer = &(layoutBuffer->getTupleBuffer().getBuffer<uint8_t>()[0]);
    auto fieldOffset = layoutBuffer->calcOffset(0, fieldIndex, boundaryChecks);

    T* basePointer = reinterpret_cast<T*>(bufferBasePointer + fieldOffset);
    return DynamicColumnLayoutField<T, boundaryChecks>(basePointer, layoutBuffer);
}

template<class T, bool boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>::create(const std::string& fieldName,
                                                    std::shared_ptr<DynamicColumnLayoutBuffer> layoutBuffer) {
    auto fieldIndex = layoutBuffer->getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value()) {
        return DynamicColumnLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layoutBuffer);
    }
    NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutField: Could not find fieldIndex for " << fieldName);
}

template<class T, bool boundaryChecks>
inline T& DynamicColumnLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks && recordIndex >= dynamicColumnLayoutBuffer->getCapacity()) {
        NES_THROW_RUNTIME_ERROR("recordIndex out of bounds!" << dynamicColumnLayoutBuffer->getCapacity()
                                                             << " >= " << recordIndex);
    }

    return *(basePointer + recordIndex);
}

}// namespace NES::Runtime::DynamicMemoryLayout

#endif// NES_INCLUDE_RUNTIME_MEMORY_LAYOUT_DYNAMIC_COLUMN_LAYOUT_FIELD_HPP_
