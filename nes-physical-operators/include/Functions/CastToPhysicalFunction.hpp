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
#include <optional>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

class CastToPhysicalFunction
{
public:
    explicit CastToPhysicalFunction(PhysicalFunction childFunction, DataType castToType);
    /// inputType: the child's type; a cast of a number, boolean or character to VARSIZED formats it as text.
    CastToPhysicalFunction(PhysicalFunction childFunction, DataType castToType, std::optional<DataType> inputType);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    DataType castToType;
    PhysicalFunction childFunction;
    std::optional<DataType> inputType;
};

static_assert(PhysicalFunctionConcept<CastToPhysicalFunction>);

/// The function's integer result as `outputType`: our integer arithmetic computes as C++ does (e.g. INT8 * INT8 in INT32),
/// and the cast gives the type the planner inferred (which wraps on overflow).
PhysicalFunction castIntegerResult(PhysicalFunction function, const DataType& outputType);

}
