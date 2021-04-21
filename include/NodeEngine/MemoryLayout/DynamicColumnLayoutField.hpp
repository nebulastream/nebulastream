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

#ifndef NES_DYNAMICCOLUMNLAYOUTFIELD_HPP
#define NES_DYNAMICCOLUMNLAYOUTFIELD_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES::NodeEngine::DynamicMemoryLayout {

/**
 * @brief This class is used for handling fields in a given DynamicColumnLayoutBuffer. It also overrides the operator[] for a more user friendly access of records for a predefined field.
 * @tparam T
 * @tparam boundaryChecks
 * This class is non-thread safe
 */
template<class T, bool boundaryChecks>
class DynamicColumnLayoutField {

  public:
    static inline DynamicColumnLayoutField<T, boundaryChecks> create(uint64_t fieldIndex,
                                                                     DynamicColumnLayoutBuffer& layoutBuffer);
    static inline DynamicColumnLayoutField<T, boundaryChecks> create(std::string fieldName,
                                                                     DynamicColumnLayoutBuffer& layoutBuffer);
    inline T& operator[](size_t recordIndex);
    DynamicColumnLayoutField(T* basePointer, DynamicColumnLayoutBuffer& dynamicColumnLayoutBuffer)
        : basePointer(basePointer), dynamicColumnLayoutBuffer(dynamicColumnLayoutBuffer){};

  private:
    T* basePointer;
    DynamicColumnLayoutBuffer& dynamicColumnLayoutBuffer;
};

template<class T, bool boundaryChecks>
inline DynamicColumnLayoutField<T, boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, DynamicColumnLayoutBuffer& layoutBuffer) {
    if (boundaryChecks && fieldIndex >= layoutBuffer.getFieldSizes().size()) {
        NES_THROW_RUNTIME_ERROR("fieldIndex out of bounds! " << layoutBuffer.getFieldSizes().size() << " >= " << fieldIndex);
    }

    auto bufferBasePointer = &(layoutBuffer.getTupleBuffer().getBufferAs<uint8_t>()[0]);
    auto fieldOffset = layoutBuffer.calcOffset(0, fieldIndex, boundaryChecks);

    T* basePointer = reinterpret_cast<T*>(bufferBasePointer + fieldOffset);
    return DynamicColumnLayoutField<T, boundaryChecks>(basePointer, layoutBuffer);
}

template<class T, bool boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>
DynamicColumnLayoutField<T, boundaryChecks>::create(std::string fieldName, DynamicColumnLayoutBuffer& layoutBuffer) {
    auto fieldIndex = layoutBuffer.getFieldIndexFromName(fieldName);
    if (fieldIndex.has_value()) {
        return DynamicColumnLayoutField<T, boundaryChecks>::create(fieldIndex.value(), layoutBuffer);
    } else {
        NES_THROW_RUNTIME_ERROR("DynamicColumnLayoutField: Could not find fieldIndex for " << fieldName);
    }
}

template<class T, bool boundaryChecks>
inline T& DynamicColumnLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks && recordIndex >= dynamicColumnLayoutBuffer.getCapacity()) {
        NES_THROW_RUNTIME_ERROR("recordIndex out of bounds!" << dynamicColumnLayoutBuffer.getCapacity()
                                                             << " >= " << recordIndex);
    }

    return *(basePointer + recordIndex);
}

}// namespace NES::NodeEngine::DynamicMemoryLayout

#endif//NES_DYNAMICCOLUMNLAYOUTFIELD_HPP