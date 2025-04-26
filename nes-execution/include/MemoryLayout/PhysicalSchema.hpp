//
// Created by ls on 4/23/25.
//

#pragma once
#include <API/Schema.hpp>
#include <PhysicalType.hpp>

namespace NES{
struct PhysicalSchema {
     std::vector<Schema::Identifier, PhysicalType> fields;
};
}
