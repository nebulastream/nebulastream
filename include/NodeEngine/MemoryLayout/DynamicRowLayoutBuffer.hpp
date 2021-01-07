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

#ifndef NES_DYNAMICROWLAYOUTBUFFER_HPP
#define NES_DYNAMICROWLAYOUTBUFFER_HPP

#include <stdint.h>
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <NodeEngine/MemoryLayout/DynamicRowLayout.hpp>

namespace NES::NodeEngine {

class DynamicRowLayoutBuffer;
typedef std::unique_ptr<DynamicRowLayoutBuffer> DynamicRowLayoutBufferPtr;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;

class DynamicRowLayoutBuffer : public DynamicLayoutBuffer {
  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicRowLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicRowLayoutPtr dynamicRowLayout);

    template<typename...  Types> std::tuple<Types...> readRecord(uint64_t recordIndex);
    template<typename... Types> void pushRecord(std::tuple<Types...> record);

  private:
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    writeTupleToBufferRowWise(std::tuple<Tp...>& t, unsigned char* address, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
    }

    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I < sizeof...(Tp), void>::type
    writeTupleToBufferRowWise(std::tuple<Tp...>& t, unsigned char* address, FieldSizesPtr fieldSizes) {
        memcpy(address, &std::get<I>(t), fieldSizes->at(I));
        writeTupleToBufferRowWise<I + 1, Tp...>(t, address + fieldSizes->at(I), fieldSizes);
    }

    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    readTupleFromBufferRowWise(std::tuple<Tp...>& t, unsigned char* address, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
    }

    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I < sizeof...(Tp), void>::type
    readTupleFromBufferRowWise(std::tuple<Tp...>& t, unsigned char* address, FieldSizesPtr fieldSizes) {
        memcpy(&std::get<I>(t), address, fieldSizes->at(I));
        readTupleFromBufferRowWise<I + 1, Tp...>(t, address + fieldSizes->at(I), fieldSizes);
    }

  private:
    DynamicRowLayoutPtr dynamicRowLayout;
};



template<typename... Types>
void DynamicRowLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    uint64_t offSet = calcOffset(numberOfRecords, 0);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicRowLayout->getFieldSizes();
    writeTupleToBufferRowWise(record, &(byteBuffer[offSet]), fieldSizes);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
}

template<typename... Types>
std::tuple<Types...> DynamicRowLayoutBuffer::readRecord(uint64_t recordIndex) {
    uint64_t offSet = calcOffset(recordIndex, 0);
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicRowLayout->getFieldSizes();

    readTupleFromBufferRowWise(retTuple, &(byteBuffer[offSet]), fieldSizes);
    return retTuple;
}

}

#endif//NES_DYNAMICROWLAYOUTBUFFER_HPP
