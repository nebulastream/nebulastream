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

#include <any>
#include <cstdint>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/Interface/Record.hpp>

#include <rapidcheck.h>

namespace NES::TestUtils
{

/// The buffer manager uses a fixed 4096-byte buffer size.
/// Schemas whose tuple size exceeds this must be discarded via RC_PRE.
static constexpr uint64_t BUFFER_SIZE = 4096;

/// Type pools for property-based tests.
/// All types including VARSIZED.
extern const std::vector<DataType::Type> ALL_KEY_TYPES;
extern const std::vector<DataType::Type> ALL_VALUE_TYPES;

/// RapidCheck generator for a non-empty vector of DataType::Type from a given pool.
rc::Gen<std::vector<DataType::Type>> genTypeSchema(std::vector<DataType::Type> pool, size_t minFields = 1, size_t maxFields = 256);

/// Estimates the schema size in bytes for a given type vector.
uint64_t estimateSchemaSize(const std::vector<DataType::Type>& types);

/// Extracts the raw C++ value from a VarVal and wraps it in std::any.
/// Supports all fixed-size numeric types and VARSIZED (extracted as std::string).
std::any varValToAny(const VarVal& val, DataType::Type type);

/// Converts a Record's fields into a std::vector<std::any> using the given projection and types.
std::vector<std::any> recordToAnyVec(
    const Record& record, const std::vector<Record::RecordFieldIdentifier>& projection, const std::vector<DataType::Type>& types);

/// Compares two std::any values of the same DataType::Type. Returns <0, 0, or >0.
int compareAnyField(const std::any& lhs, const std::any& rhs, DataType::Type type);

/// Custom comparator for std::map with std::vector<std::any> keys.
/// Compares field-by-field using the known DataType::Type per position.
struct AnyVecLess
{
    std::vector<DataType::Type> types;
    bool operator()(const std::vector<std::any>& lhs, const std::vector<std::any>& rhs) const;
};

/// Checks equality of two std::vector<std::any> using type-aware comparison.
bool anyVecsEqual(const std::vector<std::any>& lhs, const std::vector<std::any>& rhs, const std::vector<DataType::Type>& types);

}
