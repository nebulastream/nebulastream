//
// Created by ls on 11/30/24.
//

#pragma once
#include "PhysicalTypes.hpp"
#include "TupleBufferRef.hpp"


#include <cstddef>
#include <unordered_set>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include "VarVal.hpp"

namespace NES
{

class MemoryOutputAccessor
{
    virtual ~MemoryOutputAccessor() = default;
    virtual nautilus::val<bool> append(TupleBufferRef output, std::vector<Nautilus::VarVal> fields) = 0;
};

class MemoryInputAccessor
{
    virtual ~MemoryInputAccessor() = default;
    virtual std::vector<Nautilus::VarVal> read(nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) = 0;
    virtual void setBuffer(TupleBufferRef tb) = 0;
};

class MemoryRecordAccessor
{
public:
    virtual ~MemoryRecordAccessor() = default;

    virtual Nautilus::VarVal operator[](const FieldName& fieldName) = 0;
    virtual Nautilus::VarVal operator[](size_t index) = 0;
    void write(const FieldName& fieldName, Nautilus::ScalarVarVal scalar, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        write(fieldName, Nautilus::VarVal(scalar), bufferProvider);
    }
    void write(size_t index, Nautilus::ScalarVarVal scalar, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        write(index, Nautilus::VarVal(scalar), bufferProvider);
    }
    virtual void write(const FieldName& fieldName, Nautilus::VarVal, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) = 0;
    virtual void write(size_t index, Nautilus::VarVal, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider) = 0;
    virtual void advance(nautilus::val<size_t> index) = 0;
    virtual void setBuffer(TupleBufferRef tb) = 0;
};

class MemoryAccessor
{
public:
    explicit MemoryAccessor(std::unique_ptr<MemoryRecordAccessor> accessor, TupleBufferRef tb, PhysicalSchema schema)
        : buffer(tb), schema(std::move(schema)), accessor(std::move(accessor))
    {
        this->accessor->setBuffer(buffer);
    }

    MemoryRecordAccessor& operator[](nautilus::val<size_t> index)
    {
        accessor->advance(index);
        return *accessor;
    }

    void append(std::vector<Nautilus::VarVal> fields, nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
    {
        auto index = buffer.getNumberOfTuples();
        buffer.setNumberOfTuples(index + 1);
        accessor->advance(index);
        for (nautilus::static_val<size_t> i = 0; i < schema.fields.size(); ++i)
        {
            accessor->write(i, fields[i], bufferProvider);
        }
    }

    std::vector<Nautilus::VarVal> at(nautilus::val<size_t> index, std::unordered_set<FieldName> projection)
    {
        std::vector<Nautilus::VarVal> result;
        result.reserve(schema.fields.size());
        accessor->advance(index);
        for (nautilus::static_val<size_t> i = 0; i < schema.fields.size(); ++i)
        {
            if (!projection.empty() && !projection.contains(schema.fields[i].first))
            {
                continue;
            }

            result.emplace_back((*accessor)[i]);
        }
        return result;
    }

    std::vector<Nautilus::VarVal> at(nautilus::val<size_t> index) { return at(index, {}); }

    TupleBufferRef buffer;
    PhysicalSchema schema;
    std::unique_ptr<MemoryRecordAccessor> accessor;
};
}
