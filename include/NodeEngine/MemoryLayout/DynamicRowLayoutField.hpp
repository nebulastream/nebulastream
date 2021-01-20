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

class DynamicRowLayoutBuffer;

template<typename T>
class DynamicRowLayoutField {


  public:
    static inline DynamicRowLayoutField<T> create(uint64_t fieldIndex, DynamicRowLayoutBufferPtr& layoutBuffer);
    inline T operator[](size_t recordIndex);

  private:
    DynamicRowLayoutField(DynamicRowLayoutBufferPtr& dynamicRowLayoutBuffer, T* basePointer, NES::NodeEngine::FIELD_SIZE fieldIndex,
                          NES::NodeEngine::FIELD_SIZE recordSize) : dynamicRowLayoutBuffer(dynamicRowLayoutBuffer), fieldIndex(fieldIndex), recordSize(recordSize), basePointer(basePointer) {};


    DynamicRowLayoutBufferPtr& dynamicRowLayoutBuffer;
    NES::NodeEngine::FIELD_SIZE fieldIndex;
    NES::NodeEngine::FIELD_SIZE recordSize;
    T* basePointer;
};

template class DynamicRowLayoutField<int8_t>;
template class DynamicRowLayoutField<uint8_t>;
template class DynamicRowLayoutField<uint16_t>;
template class DynamicRowLayoutField<uint32_t>;

template<typename T>
inline NES::NodeEngine::DynamicRowLayoutField<T> NES::NodeEngine::DynamicRowLayoutField<T>::create(uint64_t fieldIndex, DynamicRowLayoutBufferPtr& layoutBuffer) {
    T* basePointer = &(layoutBuffer->getTupleBuffer().getBufferAs<T>()[0]) + layoutBuffer->calcOffset(0, fieldIndex);
    return DynamicRowLayoutField<T>(layoutBuffer, basePointer, fieldIndex, layoutBuffer->getRecordSize());
}

template<typename T>
inline T NES::NodeEngine::DynamicRowLayoutField<T>::operator[](size_t recordIndex) {
    T* address = basePointer + recordSize * recordIndex;
    NES_DEBUG("DynamicRowLayout: address = " << address << " basePointer = " << basePointer);
    return *(address);
}

}

#endif //NES_DYNAMICROWLAYOUTFIELD_HPP