//
// Created by ls on 11/30/24.
//

#pragma once

#include "MemoryRecordAccessor.hpp"
#include "PhysicalTypes.hpp"
#include "TupleBufferRef.hpp"
#include "VarVal.hpp"

namespace NES
{

class RowMemoryRecordAccessor : public MemoryRecordAccessor
{
    struct RowLayoutSchema
    {
        RowLayoutSchema(const PhysicalSchema& schema);
        size_t tupleSize;
        std::unordered_map<FieldName, size_t> indexes;
        std::vector<size_t> tupleOffset;
        std::vector<PhysicalType> physicalTypes;
    };

public:
    RowMemoryRecordAccessor(const PhysicalSchema& schema);

    Nautilus::VarVal operator[](const FieldName& fieldName) override;
    Nautilus::VarVal operator[](size_t index) override;
    void write(const FieldName& fieldName, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) override;
    void write(size_t index, Nautilus::VarVal v, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) override;
    void setBuffer(TupleBufferRef tb) override;
    void advance(nautilus::val<size_t> index) override;

    RowLayoutSchema layout;
    std::optional<TupleBufferRef> tb;
    nautilus::val<int8_t*> base{nullptr};
    nautilus::val<int8_t*> current{nullptr};
};

}