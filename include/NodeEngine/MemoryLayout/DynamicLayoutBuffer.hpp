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

#ifndef NES_DYNAMICLAYOUTBUFFER_HPP
#define NES_DYNAMICLAYOUTBUFFER_HPP

#include <NodeEngine/TupleBuffer.hpp>
#include <string.h>

namespace NES::NodeEngine {

typedef uint64_t FIELD_SIZE;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;

/**
 * @brief This abstract class is the base class for DynamicRowLayoutBuffer and DynamicColumnLayoutBuffer.
 * As the base class, it has multiple methods or members that are useful for both derived classes.
 */
class DynamicLayoutBuffer {

  public:
    DynamicLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity)
        : tupleBuffer(tupleBuffer), capacity(capacity), numberOfRecords(0) {}
    /**
     * @brief calculates the address/offset of ithRecord and jthField
     * @param ithRecord
     * @param jthField
     * @return
     */
    virtual uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField, bool boundaryChecks) = 0;
    uint64_t getCapacity() { return capacity; }
    uint64_t getNumberOfRecords() {return numberOfRecords; }
    TupleBuffer& getTupleBuffer() { return tupleBuffer; }


  protected:
    TupleBuffer& tupleBuffer;
    uint64_t capacity;
    uint64_t numberOfRecords = 0;
};
}


#endif//NES_DYNAMICLAYOUTBUFFER_HPP
