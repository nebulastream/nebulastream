/*
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

#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <DataTypes/DataType.hpp>
#include <substrait/plan.pb.h>
#include <substrait/type.pb.h>

/// Shared by the conversions of logical (SubstraitToLogicalPlan) and physical (SubstraitToPhysicalPlan) plans.
namespace NES::Calcite::detail
{
/// Type anchor -> our type, for the user-defined types a plan declares; see unsignedTypes().
using UserDefinedTypes = std::unordered_map<uint32_t, DataType::Type>;

/// Substrait has no unsigned integers; Calcite's UTINYINT..UBIGINT arrive as the user-defined types u8..u64 of
/// extension:io.substrait:unsigned_integers (see Unsigned.java).
UserDefinedTypes unsignedTypes(const substrait::Plan& plan);

DataType toDataType(const substrait::Type& type, const UserDefinedTypes& userDefinedTypes);

/// Function anchor -> function name without its signature (gt, not gt:any_any).
std::unordered_map<uint32_t, std::string> functionNames(const substrait::Plan& plan);
}
