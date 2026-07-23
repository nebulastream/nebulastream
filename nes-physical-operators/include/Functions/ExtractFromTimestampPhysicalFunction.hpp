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

#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/ExtractFromTimestampLogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

/// Extracts a calendar component (day-of-month, month, or year) from a unix timestamp in
/// milliseconds (UINT64) and returns it as UINT64. The component is selected via `unit`.
class ExtractFromTimestampPhysicalFunction
{
public:
    ExtractFromTimestampPhysicalFunction(TimestampUnit unit, PhysicalFunction childFunction, DataType outputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

    static PhysicalFunctionRegistryReturnType createDay_Of(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createMonth_Of(PhysicalFunctionRegistryArguments arguments);
    static PhysicalFunctionRegistryReturnType createYear_Of(PhysicalFunctionRegistryArguments arguments);

private:
    TimestampUnit unit;
    DataType outputType;
    PhysicalFunction childFunction;
};

static_assert(PhysicalFunctionConcept<ExtractFromTimestampPhysicalFunction>);

}
