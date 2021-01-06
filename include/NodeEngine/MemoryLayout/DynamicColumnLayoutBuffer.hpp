//
// Created by nils on 05.01.21.
//

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
    template<typename... Types> std::tuple<Types...> readRecord(uint64_t recordIndex);
    template<typename... Types> void pushRecord(std::tuple<Types...> record);

  private:
    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    writeTupleToBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
        ((void)ithRecord);
    }

    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I < sizeof...(Tp), void>::type
    writeTupleToBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        auto fieldAddress = address + calcOffset(ithRecord, I);
        NES_DEBUG("DynamicLayoutBuffer.writeTupleToBufferColumnWise: address = " << std::to_string((uint64_t)address) << " I = " << I);
        memcpy(fieldAddress, &std::get<I>(t), fieldSizes->at(I));
        writeTupleToBufferColumnWise<I + 1, Tp...>(t, address, ithRecord, fieldSizes);
    }

    template<std::size_t I = 0, typename... Tp>
    inline typename std::enable_if<I == sizeof...(Tp), void>::type
    readTupleFromBufferColumnWise(std::tuple<Tp...>& t, unsigned char* address, uint64_t ithRecord, FieldSizesPtr fieldSizes) {
        ((void)t);
        ((void)address);
        ((void)fieldSizes);
        ((void)ithRecord);
    }

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
    writeTupleToBufferColumnWise(record, &(byteBuffer[0]), numberOfRecords, fieldSizes);

    tupleBuffer.setNumberOfTuples(++numberOfRecords);
}

template<typename... Types>
std::tuple<Types...> DynamicColumnLayoutBuffer::readRecord(uint64_t recordIndex) {
    auto byteBuffer = tupleBuffer.getBufferAs<uint8_t>();

    std::tuple<Types...> retTuple;
    auto fieldSizes = dynamicColLayout->getFieldSizes();

    readTupleFromBufferColumnWise(retTuple, &(byteBuffer[0]), recordIndex, fieldSizes);
    return retTuple;
}

}

#endif//NES_DYNAMICCOLUMNLAYOUTBUFFER_HPP
