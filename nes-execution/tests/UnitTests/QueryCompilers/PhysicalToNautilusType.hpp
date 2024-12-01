//
// Created by ls on 11/30/24.
//

#pragma once

#include "PhysicalTypes.hpp"
namespace NES
{

template <typename T>
struct PhysicalNautilusMapping
{
};

#define ADD_MAPPING(Physical, NautilusT) \
    template <> \
    struct PhysicalNautilusMapping<Physical> \
    { \
        using NautilusType = nautilus::val<NautilusT>; \
        using PointerType = NautilusT*; \
        using BasicType = NautilusT; \
    }

ADD_MAPPING(UInt64, uint64_t);
ADD_MAPPING(UInt8, uint8_t);
ADD_MAPPING(Int8, int8_t);
ADD_MAPPING(Char, char);

}