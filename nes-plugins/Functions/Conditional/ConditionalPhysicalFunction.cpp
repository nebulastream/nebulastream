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

#include <Functions/ConditionalPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <static.hpp>

namespace NES
{
namespace
{
/// Groups the leading elements of the flat child list into (when, then) pairs, leaving out the
/// trailing default result if there is one.
std::vector<WhenThenPhysicalFunction> groupWhenThens(const std::vector<PhysicalFunction>& children)
{
    std::vector<WhenThenPhysicalFunction> whenThens;
    whenThens.reserve(children.size() / 2);
    for (size_t index = 0; index + 1 < children.size(); index += 2)
    {
        whenThens.push_back(WhenThenPhysicalFunction{.when = children[index], .then = children[index + 1]});
    }
    return whenThens;
}

/// The NULL that a CASE without ELSE returns when no condition holds.
VarVal nullOf(const DataType& type)
{
    const nautilus::val<bool> isNull{true};
    switch (type.type)
    {
        case DataType::Type::BOOLEAN:
            return VarVal{nautilus::val<bool>{false}, true, isNull};
        case DataType::Type::INT8:
            return VarVal{nautilus::val<int8_t>{0}, true, isNull};
        case DataType::Type::INT16:
            return VarVal{nautilus::val<int16_t>{0}, true, isNull};
        case DataType::Type::INT32:
            return VarVal{nautilus::val<int32_t>{0}, true, isNull};
        case DataType::Type::INT64:
            return VarVal{nautilus::val<int64_t>{0}, true, isNull};
        case DataType::Type::CHAR:
            return VarVal{nautilus::val<char>{0}, true, isNull};
        case DataType::Type::UINT8:
            return VarVal{nautilus::val<uint8_t>{0}, true, isNull};
        case DataType::Type::UINT16:
            return VarVal{nautilus::val<uint16_t>{0}, true, isNull};
        case DataType::Type::UINT32:
            return VarVal{nautilus::val<uint32_t>{0}, true, isNull};
        case DataType::Type::UINT64:
            return VarVal{nautilus::val<uint64_t>{0}, true, isNull};
        case DataType::Type::FLOAT32:
            return VarVal{nautilus::val<float>{0}, true, isNull};
        case DataType::Type::FLOAT64:
            return VarVal{nautilus::val<double>{0}, true, isNull};
        case DataType::Type::VARSIZED:
            return VarVal{VariableSizedData{nautilus::val<int8_t*>{nullptr}, nautilus::val<uint64_t>{0}}, true, isNull};
        case DataType::Type::UNDEFINED:
            break;
    }
    throw UnknownDataType("CASE WHEN cannot produce a NULL of type {}", type);
}

/// Branches may disagree on whether they can produce a null, while the expression as a whole has
/// one declared type. Rebuilding the result pins it to that type instead of to the branch that
/// produced it.
VarVal withDeclaredType(const VarVal& value, const DataType& resultType)
{
    return value.customVisit([&]<typename T>(const T& raw) { return VarVal{raw, resultType.nullable, value.isNull()}; });
}
}

ConditionalPhysicalFunction::ConditionalPhysicalFunction(
    std::vector<WhenThenPhysicalFunction> whenThens, std::optional<PhysicalFunction> elseCase, DataType resultType)
    : whenThens(std::move(whenThens)), elseCase(std::move(elseCase)), resultType(std::move(resultType))
{
}

/// The expression has a single exit: every branch assigns the result, and the conditions after the first one that holds
/// are not evaluated. Returning from inside the unrolled loop instead gives every branch its own exit, and the trace
/// then continues separately after each of them, so a pipeline with n CASE expressions traced 2^n paths: 10 CASEs in
/// one projection took about a minute to trace. The placeholder carries the declared type, so every assignment keeps
/// the same type and nullability on all traced paths.
VarVal ConditionalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    VarVal result = withDeclaredType(nullOf(resultType), resultType);
    nautilus::val<bool> matched = false;
    for (const auto& [when, then] : nautilus::static_iterable(whenThens))
    {
        if (not matched)
        {
            /// A condition that is null is not true, so it does not select its branch. Converting the
            /// condition to a bool already accounts for that.
            if (when.execute(record, arena))
            {
                result = withDeclaredType(then.execute(record, arena), resultType);
                matched = true;
            }
        }
    }
    /// Whether there is a default is known while tracing, so it is checked on the host side: the overloaded `and` of a
    /// traced bool does not short-circuit.
    if (elseCase.has_value())
    {
        if (not matched)
        {
            result = withDeclaredType(elseCase->execute(record, arena), resultType);
        }
    }
    return result;
}

PhysicalFunctionRegistryReturnType ConditionalPhysicalFunction::createConditional(PhysicalFunctionRegistryArguments arguments)
{
    /// The logical function already validated the argument shape and types, so a bad shape here is a lowering bug.
    const auto& children = arguments.childFunctions;
    PRECONDITION(children.size() >= 2, "ConditionalPhysicalFunction requires at least one when/then pair, but got {} children", children.size());
    const auto elseCase = children.size() % 2 == 1 ? std::optional{children.back()} : std::nullopt;
    return ConditionalPhysicalFunction{groupWhenThens(children), elseCase, arguments.outputType};
}
}
