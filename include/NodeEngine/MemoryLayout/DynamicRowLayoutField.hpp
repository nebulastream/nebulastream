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

#ifndef NES_DYNAMICROWLAYOUTFIELD_HPP
#define NES_DYNAMICROWLAYOUTFIELD_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

/**
 * @brief This class is used for handling fields in a given DynamicColumnLayoutBuffer. It also overrides the operator[] for a more user friendly access of records for a predefined field.
 * @tparam T
 * @tparam boundaryChecks
 * @caution This class is non-thread safe
 */
template<class T, bool boundaryChecks>
class DynamicRowLayoutField {

  public:
    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldIndex and a layoutBuffer
     */
    static inline DynamicRowLayoutField<T, boundaryChecks> create(uint64_t fieldIndex,
                                                                  std::shared_ptr<DynamicRowLayoutBuffer> layoutBuffer);

    /**
     * @param fieldIndex
     * @param layoutBuffer
     * @tparam boundaryChecks if true will check if access is allowed
     * @tparam T type of field
     * @return field handler via a fieldName and a layoutBuffer
     */
    static inline DynamicRowLayoutField<T, boundaryChecks> create(std::string fieldName,
                                                                  std::shared_ptr<DynamicRowLayoutBuffer> layoutBuffer);

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
    DynamicRowLayoutField(std::shared_ptr<DynamicRowLayoutBuffer> dynamicRowLayoutBuffer,
                          uint8_t* basePointer,
                          FIELD_SIZE fieldIndex,
                          FIELD_SIZE recordSize)
        : fieldIndex(fieldIndex), recordSize(recordSize), basePointer(basePointer),
          dynamicRowLayoutBuffer(dynamicRowLayoutBuffer){};

    const FIELD_SIZE fieldIndex;
    const FIELD_SIZE recordSize;
    uint8_t* basePointer;
    DynamicRowLayoutBufferPtr dynamicRowLayoutBuffer;
};

template<class T, bool boundaryChecks>
inline DynamicRowLayoutField<T, boundaryChecks>
DynamicRowLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, std::shared_ptr<DynamicRowLayoutBuffer> layoutBuffer) {
    if (boundaryChecks && fieldIndex >= layoutBuffer->getFieldOffSets().size()) {
        NES_THROW_RUNTIME_ERROR("fieldIndex out of bounds!" << layoutBuffer->getFieldOffSets().size() << " >= " << fieldIndex);
    }

    // via pointer arithmetic gets the starting field address
    auto bufferBasePointer = &(layoutBuffer->getTupleBuffer().getBuffer<uint8_t>()[0]);
    auto offSet = layoutBuffer->calcOffset(0, fieldIndex, boundaryChecks);
    auto basePointer = bufferBasePointer + offSet;

    return DynamicRowLayoutField<T, boundaryChecks>(layoutBuffer, basePointer, fieldIndex, layoutBuffer->getRecordSize());
}

template<class T, bool boundaryChecks>
inline DynamicRowLayoutField<T, boundaryChecks>
DynamicRowLayoutField<T, boundaryChecks>::create(std::string fieldName, std::shared_ptr<DynamicRowLayoutBuffer> layoutBuffer) {
    auto fieldIndex = layoutBuffer->getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value()) {
        return DynamicRowLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layoutBuffer);
    } else {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutField: Could not find fieldIndex for " << fieldName);
    }
}

template<class T, bool boundaryChecks>
inline T& DynamicRowLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks && recordIndex >= dynamicRowLayoutBuffer->getCapacity()) {
        NES_THROW_RUNTIME_ERROR("recordIndex out of bounds!" << dynamicRowLayoutBuffer->getCapacity() << " >= " << recordIndex);
    }

    return *reinterpret_cast<T*>(basePointer + recordSize * recordIndex);
}

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICROWLAYOUTFIELD_HPP