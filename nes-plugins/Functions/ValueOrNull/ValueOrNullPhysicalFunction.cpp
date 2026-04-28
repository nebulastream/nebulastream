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

#include "ValueOrNullPhysicalFunction.hpp"

#include <utility>

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <val_bool.hpp>

namespace NES
{

ValueOrNullPhysicalFunction::ValueOrNullPhysicalFunction(PhysicalFunction predicate, PhysicalFunction value)
    : predicate(std::move(predicate)), value(std::move(value))
{
}

/// value_or_null(predicate, value):
///   predicate is true and not null  -> value (which itself may be null)
///   otherwise                       -> NULL
/// The result is always nullable so downstream MAX/SUM/etc. ignore the dropped rows.
VarVal ValueOrNullPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    const auto predicateVal = predicate.execute(record, arena);
    const auto valueVal = value.execute(record, arena);

    const auto predicateRaw = predicateVal.getRawValueAs<nautilus::val<bool>>();
    const auto resultIsNull = predicateVal.isNull() or not predicateRaw or valueVal.isNull();

    return valueVal.customVisit(
        [resultIsNull]<typename Underlying>(const Underlying& underlying) -> VarVal { return VarVal{underlying, true, resultIsNull}; });
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::Registervalue_or_nullPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2, "value_or_null function must have exactly two child functions");
    return ValueOrNullPhysicalFunction(arguments.childFunctions[0], arguments.childFunctions[1]);
}

}
