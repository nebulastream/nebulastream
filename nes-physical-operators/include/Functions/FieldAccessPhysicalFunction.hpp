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

#include <string>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Reads a (possibly compound) field from the record. The suffixes vector
/// describes the physical layout of the field — `{""}` for a scalar field
/// (the default), `{".x", ".y", ".z"}` for a Point-shaped compound, etc.
/// The lowering phase fills in the suffix list based on the field's logical
/// type's physical layout.
class FieldAccessPhysicalFunction
{
public:
    explicit FieldAccessPhysicalFunction(Record::RecordFieldIdentifier field);
    FieldAccessPhysicalFunction(Record::RecordFieldIdentifier field, std::vector<std::string> suffixes);
    [[nodiscard]] Value execute(const Record& record, ArenaRef& arena) const;

private:
    Record::RecordFieldIdentifier field;
    std::vector<std::string> suffixes;
};

static_assert(PhysicalFunctionConcept<FieldAccessPhysicalFunction>);


}
