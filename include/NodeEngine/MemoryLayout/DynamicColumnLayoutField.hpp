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


template<typename T>
class DynamicColumnLayoutField {


  public:
    static inline DynamicColumnLayoutField<T> create(uint64_t fieldIndex, DynamicColumnLayoutBufferPtr& layoutBuffer);
    inline T operator[](size_t recordIndex);

  private:
  public:
    DynamicColumnLayoutField(T* basePointer) : basePointer(basePointer) {};
  private:
    T* basePointer;
};

template<typename T>
inline NES::NodeEngine::DynamicColumnLayoutField<T> NES::NodeEngine::DynamicColumnLayoutField<T>::create(uint64_t fieldIndex, DynamicColumnLayoutBufferPtr& layoutBuffer) {
    T* basePointer = reinterpret_cast<T*>(&(layoutBuffer->getTupleBuffer().getBufferAs<uint8_t>()[0]) + layoutBuffer->calcOffset(0, fieldIndex));
    NES_DEBUG("DynamicRowLayout: basePointer = " << std::hex << basePointer);
    return DynamicColumnLayoutField<T>(basePointer);
}

template<typename T>
inline T NES::NodeEngine::DynamicColumnLayoutField<T>::operator[](size_t recordIndex) {
    return *(basePointer + recordIndex);
}

}

#endif //NES_DYNAMICCOLUMNLAYOUTFIELD_HPP