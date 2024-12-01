//
// Created by ls on 11/30/24.
//

#pragma once
#include <cstddef>
#include <cstdint>
#include <unordered_set>
#include <variant>
#include <API/Schema.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/Engine.hpp>
#include <nautilus/std/ostream.h>
#include <nautilus/std/sstream.h>
#include <nautilus/std/string.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <std/cstring.h>
#include "PhysicalToNautilusType.hpp"
#include "VarVal.hpp"

namespace NES {
struct TupleBufferRef
{
    template <typename T>
    nautilus::val<T*> getBuffer()
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getBuffer<T>(); }, tb);
    }

    nautilus::val<size_t> getBufferSize() const
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getBufferSize(); }, tb);
    }

    void setNumberOfTuples(nautilus::val<size_t> newNumberOfTuples) const
    {
        return nautilus::invoke(
            +[](Memory::TupleBuffer* tb, size_t newNumberOfTuples) { return tb->setNumberOfTuples(newNumberOfTuples); },
            tb,
            newNumberOfTuples);
    }

    nautilus::val<size_t> getNumberOfTuples() const
    {
        return nautilus::invoke(+[](Memory::TupleBuffer* tb) { return tb->getNumberOfTuples(); }, tb);
    }

    std::tuple<nautilus::val<unsigned>, Nautilus::ScalarVarValPtr, nautilus::val<size_t>>
    allocateChildBuffer(PhysicalScalarType scalarType, nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        return std::visit(
            [&]<typename T>(T)
            {
                auto [index, ptr, size] = allocateChildBuffer<typename PhysicalNautilusMapping<T>::BasicType>(provider);
                return std::make_tuple(index, Nautilus::ScalarVarValPtr(ptr), size);
            },
            scalarType.value);
    }

    template <typename T>
    std::tuple<nautilus::val<unsigned>, nautilus::val<T*>, nautilus::val<size_t>>
    allocateChildBuffer(nautilus::val<Memory::AbstractBufferProvider*> provider)
    {
        auto index = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, Memory::AbstractBufferProvider* provider)
            {
                auto buffer = provider->getBufferBlocking();
                return tb->storeChildBuffer(buffer);
            },
            tb,
            provider);
        auto size = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index) { return tb->loadChildBuffer(index).getBufferSize(); }, tb, index);
        auto data = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index) { return tb->loadChildBuffer(index).getBuffer<T>(); }, tb, index);
        return {index, data, size};
    }

    std::tuple<Nautilus::ScalarVarValPtr, nautilus::val<size_t>>
    loadChildBuffer(PhysicalScalarType scalarType, nautilus::val<unsigned> index)
    {
        return std::visit(
            [&]<typename T>(T)
            {
                auto [ptr, size] = loadChildBuffer<typename PhysicalNautilusMapping<T>::BasicType>(index);
                return std::make_tuple(Nautilus::ScalarVarValPtr(ptr), size);
            },
            scalarType.value);
    }

    template <typename T>
    std::tuple<nautilus::val<T*>, nautilus::val<size_t>> loadChildBuffer(nautilus::val<unsigned> index)
    {
        auto size = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index)
            {
                auto buffer = tb->loadChildBuffer(index);
                return buffer.getBufferSize();
            },
            tb,
            index);

        auto data = nautilus::invoke(
            +[](Memory::TupleBuffer* tb, unsigned index)
            {
                auto buffer = tb->loadChildBuffer(index);
                return buffer.getBuffer<T>();
            },
            tb,
            index);

        return {data, size};
    }

    explicit TupleBufferRef(nautilus::val<Memory::TupleBuffer*> tb) : tb(std::move(tb)) { }

private:
    nautilus::val<Memory::TupleBuffer*> tb;
};
}