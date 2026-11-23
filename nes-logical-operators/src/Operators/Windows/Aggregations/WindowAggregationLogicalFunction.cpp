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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>

namespace NES
{

TypedLogicalFunction<FieldAccessLogicalFunction> inferFieldAccess(AggregationFieldAccess field, const Schema<Field, Unordered>& schema)
{
    TypeInferenceContext context{schema};
    return std::visit(
        [&context](const auto& fieldAccessLogicalFunction)
        { return context.inferType(fieldAccessLogicalFunction).template getAs<FieldAccessLogicalFunction>(); },
        field);
}

bool aggregationFieldAccessEqual(const AggregationFieldAccess& lhs, const AggregationFieldAccess& rhs)
{
    if (lhs.index() != rhs.index())
    {
        return false;
    }
    return std::visit(
        [](const auto& lhsFn, const auto& rhsFn) -> bool
        {
            if constexpr (std::is_same_v<std::decay_t<decltype(lhsFn)>, std::decay_t<decltype(rhsFn)>>)
            {
                return StructuralFunctionEqual{}(lhsFn, rhsFn);
            }
            return false;
        },
        lhs,
        rhs);
}

std::size_t aggregationFieldAccessHash(const AggregationFieldAccess& field) noexcept
{
    return std::visit(
        [](const auto& fn) -> std::size_t { return fn ? std::hash<std::remove_cvref_t<decltype(*fn)>>{}(*fn) : 0; }, field);
}
}
