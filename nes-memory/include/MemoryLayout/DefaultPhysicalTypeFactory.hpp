//
// Created by ls on 11/24/24.
//

#pragma once

namespace NES
{

struct PhysicalType
{
    size_t size;
    bool operator==(const PhysicalType&) const = default;
};

class DefaultPhysicalTypeFactory
{
public:
    PhysicalType getPhysicalType(const DataType& data) const;
};
}