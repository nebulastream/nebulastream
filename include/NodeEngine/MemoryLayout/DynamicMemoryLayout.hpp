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

#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>


namespace NES::NodeEngine {

class DynamicMemoryLayout;
typedef std::shared_ptr<DynamicMemoryLayout> DynamicMemoryLayoutPtr;
typedef uint64_t FIELD_SIZE;


class DynamicMemoryLayout {

  public:
    DynamicMemoryLayout(bool checkBoundaryFieldChecks, uint64_t recordSize,
                        std::vector<FIELD_SIZE>& fieldSizes);

    virtual DynamicMemoryLayoutPtr copy() const = 0;
    /**
     * Maps a memoryLayout (column or row) to a tupleBuffer
     * @param tupleBuffer
     * @return
     */
    virtual std::unique_ptr<DynamicLayoutBuffer> map(TupleBuffer& tupleBuffer) = 0;


    bool isCheckBoundaryFieldChecks() const;
    uint64_t getRecordSize() const;
    const std::vector<FIELD_SIZE>& getFieldSizes() const;

  protected:

    explicit DynamicMemoryLayout();
    bool checkBoundaryFieldChecks;
    uint64_t recordSize;
    std::vector<FIELD_SIZE> fieldSizes;
};

}
#endif//NES_DYNAMICMEMORYLAYOUT_HPP
