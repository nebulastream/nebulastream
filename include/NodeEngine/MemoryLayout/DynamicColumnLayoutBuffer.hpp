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

#ifndef NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
#define NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP

#include "DynamicColumnLayout.hpp"
#include <NodeEngine/MemoryLayout/DynamicLayoutBuffer.hpp>
#include <stdint.h>

namespace NES::NodeEngine {

class DynamicColumnLayoutBuffer;
typedef std::unique_ptr<DynamicColumnLayoutBuffer> DynamicColumnLayoutBufferPtr;

typedef uint64_t COL_OFFSET_SIZE;
typedef std::shared_ptr<std::vector<NES::NodeEngine::FIELD_SIZE>> FieldSizesPtr;

class DynamicColumnLayoutBuffer : public DynamicLayoutBuffer{

  public:
    uint64_t calcOffset(uint64_t ithRecord, uint64_t jthField) override;
    DynamicColumnLayoutBuffer(TupleBuffer& tupleBuffer, uint64_t capacity, DynamicColumnLayoutPtr dynamicColLayout, std::vector<COL_OFFSET_SIZE> columnOffsets);

    /**
     * Calling this function will result in reading record at recordIndex in the tupleBuffer associated with this layoutBuffer
     * @tparam Types
     * @param record
     */
    template<typename... Types> std::tuple<Types...> readRecord(uint64_t recordIndex);

    /**
     * Calling this function will result in adding record in the tupleBuffer associated with this layoutBuffer
     * @tparam Types
     * @param record
     */
    template<typename... Types> void pushRecord(std::tuple<Types...> record);

  private:
    /**
     * @brief This function writes a record/tuple in a column layout via a template recursion.
     * @tparam I
     * @tparam Tp
     * @param t
     * @param address
     * @param ithRecord
     * @param fieldSizes
     * @return
     */
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    writeTupleToBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
        ((void)ithRecord);
    }

    /**
     * @brief This function writes a record/tuple in a column layout via a template recursion.
     * @tparam I
     * @tparam Tp
     * @param t
     * @param address
     * @param ithRecord
     * @param fieldSizes
     * @return
     */
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I < sizeof...(Tp), void>::type
    writeTupleToBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        auto fieldAddress = address + calcOffset(ithRecord, I);
        NES_DEBUG("DynamicLayoutBuffer.writeTupleToBufferColumnWise: address = " << std::to_string((uint64_t)address) << " I = " << I);
        memcpy(fieldAddress, &std::get<I>(t), fieldSizes->at(I));
        writeTupleToBufferColumnWise<I + 1, Tp...>(t, address, ithRecord, fieldSizes);
    }

    /**
     * @brief This function reads ithRecord record/tuple in a column layout via a template recursion.
     * @tparam I
     * @tparam Tp
     * @param t
     * @param address
     * @param ithRecord
     * @param fieldSizes
     * @return ithRecord tuple/record
     */
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    readTupleFromBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
        ((void)ithRecord);
    }

    /**
     * @brief This function reads ithRecord record/tuple in a column layout via a template recursion.
     * @tparam I
     * @tparam Tp
     * @param t
     * @param address
     * @param ithRecord
     * @param fieldSizes
     * @return ithRecord tuple/record
     */
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I < sizeof...(Tp), void>::type
    readTupleFromBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        auto fieldAddress = address + calcOffset(ithRecord, I);
        NES_DEBUG("DynamicLayoutBuffer.readTupleFromBufferColumnWise: address = " << std::to_string((uint64_t)address) << " I = " << I);
        memcpy(&std::get<I>(t), fieldAddress, fieldSizes->at(I));
        readTupleFromBufferColumnWise<I + 1, Tp...>(t, address, ithRecord, fieldSizes);
    }


  private:
    std::vector<COL_OFFSET_SIZE> columnOffsets;
    DynamicColumnLayoutPtr dynamicColLayout;
};



template<typename... Types>
void DynamicColumnLayoutBuffer::pushRecord(std::tuple<Types...> record) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();
    auto fieldSizes = dynamicColLayout->getFieldSizes();
//    writeTupleToBufferColumnWise(record, &(byteBuffer[0]), numberOfRecords, fieldSizes);

    auto address = &(byteBuffer[0]);
    size_t I = 0;
    auto fieldAddress = address + calcOffset(numberOfRecords, I);
    std::apply([&I, &address, &fieldAddress, fieldSizes, this](auto&&... args) {((fieldAddress = address + calcOffset(numberOfRecords, I), memcpy(fieldAddress, &args, fieldSizes->at(I)), ++I), ...);}, record);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
}

template<typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicColLayout->getFieldSizes();
//    readTupleFromBufferColumnWise(retTuple, &(byteBuffer[0]), recordIndex, fieldSizes);

    auto address = &(byteBuffer[0]);
    size_t I = 0;
    unsigned char* fieldAddress;
    std::apply([&I, address, &fieldAddress, recordIndex, fieldSizes, this](auto&&... args) {((fieldAddress = address + calcOffset(recordIndex, I), memcpy(&args, fieldAddress, fieldSizes->at(I)), ++I), ...);}, retTuple);


    return retTuple;
}

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
