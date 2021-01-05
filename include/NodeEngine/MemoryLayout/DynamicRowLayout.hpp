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

#ifndef NES_DYNAMICROWLAYOUT_HPP
#define NES_DYNAMICROWLAYOUT_HPP

#include <NodeEngine/MemoryLayout/DynamicMemoryLayout.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <NodeEngine/NodeEngine.hpp>


namespace NES {

class ArrayPhysicalField;
typedef std::shared_ptr<ArrayPhysicalField> ArrayPhysicalFieldPtr;

typedef uint64_t FIELD_SIZE;

class DynamicRowLayout;
typedef std::shared_ptr<DynamicRowLayout> DynamicRowLayoutPtr;

class DynamicRowLayout : public DynamicMemoryLayout{

  public:
    DynamicMemoryLayoutPtr copy() const override;
    DynamicRowLayout(bool checkBoundaries, SchemaPtr schema);
    static DynamicRowLayoutPtr create(SchemaPtr schema, bool checkBoundaries);
    std::unique_ptr<DynamicLayoutBuffer> map(TupleBuffer& tupleBuffer) override;


  private:
    uint64_t recordSize;
    std::shared_ptr<std::vector<FIELD_SIZE>> fieldSizes;
};


}

#endif//NES_DYNAMICROWLAYOUT_HPP
