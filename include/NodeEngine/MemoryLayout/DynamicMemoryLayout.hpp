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

#ifndef NES_DYNAMICMEMORYLAYOUT_HPP
#define NES_DYNAMICMEMORYLAYOUT_HPP

#include <NodeEngine/MemoryLayout/ArrayPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/BasicPhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalField.hpp>
#include <NodeEngine/MemoryLayout/PhysicalSchema.hpp>

namespace NES {

class DynamicMemoryLayout;
typedef std::shared_ptr<DynamicMemoryLayout> DynamicMemoryLayoutPtr;


class DynamicMemoryLayout {

  public:
    virtual DynamicMemoryLayoutPtr copy() const = 0;

    /**
     * @brief calculates the address/offset of ithRecord and jthField
     * @param ithRecord
     * @param jthField
     * @return
     */
    virtual uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) = 0;

    uint64_t getCapacity() { return capacity; }
    uint64_t getNumberOfRecords() {return numberOfRecords; }


  protected:
    explicit DynamicMemoryLayout();
    bool checkBoundaryFieldChecks;
    uint64_t capacity;
    uint64_t numberOfRecords;

};

}
#endif//NES_DYNAMICMEMORYLAYOUT_HPP
