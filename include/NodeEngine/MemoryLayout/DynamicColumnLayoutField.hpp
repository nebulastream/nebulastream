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

namespace NES::NodeEngine {

/**
 * @brief This class is used for handling fields in a given DynamicColumnLayoutBuffer. It also overrides the operator[] for a more user friendly access of records for a predefined field.
 * @tparam T
 * @tparam boundaryChecks
 */
template <class T, bool boundaryChecks>
class DynamicColumnLayoutField {

  public:
    static inline DynamicColumnLayoutField<T, boundaryChecks> create(uint64_t fieldIndex, DynamicColumnLayoutBufferPtr& layoutBuffer);
    inline T& operator[](size_t recordIndex);

  private:
  public:
    DynamicColumnLayoutField(T* basePointer, DynamicColumnLayoutBufferPtr& dynamicColumnLayoutBuffer) : basePointer(basePointer), dynamicColumnLayoutBuffer(dynamicColumnLayoutBuffer) {};
  private:
    T* basePointer;
    DynamicColumnLayoutBufferPtr& dynamicColumnLayoutBuffer;
};

template <class T, bool boundaryChecks>
inline NES::NodeEngine::DynamicColumnLayoutField<T, boundaryChecks> NES::NodeEngine::DynamicColumnLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, DynamicColumnLayoutBufferPtr& layoutBuffer) {
    if (boundaryChecks) NES_VERIFY(fieldIndex < layoutBuffer->getFieldSizes().size(), "fieldIndex out of bounds! " << layoutBuffer->getFieldSizes().size() << " >= " << fieldIndex);

    T* basePointer = reinterpret_cast<T*>(&(layoutBuffer->getTupleBuffer().getBufferAs<uint8_t>()[0]) + layoutBuffer->calcOffset(0, fieldIndex, boundaryChecks));
    NES_DEBUG("DynamicRowLayout: basePointer = " << std::hex << basePointer);
    return DynamicColumnLayoutField<T, boundaryChecks>(basePointer, layoutBuffer);
}

template <class T, bool boundaryChecks>
inline T& NES::NodeEngine::DynamicColumnLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks) NES_VERIFY(recordIndex < dynamicColumnLayoutBuffer->getCapacity(), "recordIndex out of bounds!" << dynamicColumnLayoutBuffer->getCapacity() << " >= " << recordIndex);

    return *(basePointer + recordIndex);
}

}

#endif //NES_DYNAMICCOLUMNLAYOUTFIELD_HPP