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

#ifndef NES_DYNAMICCOLUMNLAYOUT_HPP
#define NES_DYNAMICCOLUMNLAYOUT_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/NodeEngine.hpp>


namespace NES {

typedef uint64_t FIELD_SIZE;
typedef uint64_t COL_OFFSET_SIZE;

class DynamicColumnLayout;
typedef std::shared_ptr<DynamicColumnLayout> DynamicColumnLayoutPtr;

class DynamicColumnLayout : DynamicMemoryLayout{

  public:
    DynamicMemoryLayoutPtr copy() const override;
    DynamicColumnLayout(uint64_t capacity, bool checkBoundaries, SchemaPtr schema);
    static DynamicColumnLayoutPtr create(SchemaPtr schema, uint64_t bufferSize, bool checkBoundaries);
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;

  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    std::vector<FIELD_SIZE> fieldSizes;
};

}

#endif//NES_DYNAMICCOLUMNLAYOUT_HPP
