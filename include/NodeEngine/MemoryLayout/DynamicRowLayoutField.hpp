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

namespace NES::NodeEngine {


template <class T, bool boundaryChecks>
class DynamicRowLayoutField {


  public:
    static inline DynamicRowLayoutField<T, boundaryChecks> create(uint64_t fieldIndex, DynamicRowLayoutBufferPtr& layoutBuffer);
    inline T& operator[](size_t recordIndex);


  private:
    DynamicRowLayoutField(DynamicRowLayoutBufferPtr& dynamicRowLayoutBuffer, uint8_t* basePointer, NES::NodeEngine::FIELD_SIZE fieldIndex,
                          NES::NodeEngine::FIELD_SIZE recordSize) : fieldIndex(fieldIndex), recordSize(recordSize), basePointer(basePointer), dynamicRowLayoutBuffer(dynamicRowLayoutBuffer) {};


    const NES::NodeEngine::FIELD_SIZE fieldIndex;
    const NES::NodeEngine::FIELD_SIZE recordSize;
    uint8_t* basePointer;
    const DynamicRowLayoutBufferPtr& dynamicRowLayoutBuffer;
};

template <class T, bool boundaryChecks>
inline NES::NodeEngine::DynamicRowLayoutField<T, boundaryChecks> NES::NodeEngine::DynamicRowLayoutField<T, boundaryChecks>::create(uint64_t fieldIndex, DynamicRowLayoutBufferPtr& layoutBuffer) {
    if (boundaryChecks) NES_VERIFY(layoutBuffer->getFieldOffSets().size() > fieldIndex, "fieldIndex out of bounds!" << layoutBuffer->getFieldOffSets().size() << " >= " << fieldIndex);

    uint8_t* basePointer = &(layoutBuffer->getTupleBuffer().getBufferAs<uint8_t>()[0]) + layoutBuffer->calcOffset(0, fieldIndex, boundaryChecks);
    NES_DEBUG("DynamicRowLayout: basePointer = " << std::hex << basePointer);
    return DynamicRowLayoutField<T, boundaryChecks>(layoutBuffer, basePointer, fieldIndex, layoutBuffer->getRecordSize());
}

template <class T, bool boundaryChecks>
inline T& NES::NodeEngine::DynamicRowLayoutField<T, boundaryChecks>::operator[](size_t recordIndex) {
    if (boundaryChecks) NES_VERIFY(dynamicRowLayoutBuffer->getCapacity() > recordIndex, "recordIndex out of bounds!" << dynamicRowLayoutBuffer->getCapacity() << " >= " << recordIndex);

    return *reinterpret_cast<T*>(basePointer + recordSize * recordIndex);
}

}

#endif //NES_DYNAMICROWLAYOUTFIELD_HPP