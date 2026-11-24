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
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Sources/AsyncOperator.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
[[nodiscard]] inline AsyncArgumentsWithConstants makeAsyncArgumentsWithConstants(const std::vector<LogicalFunction>& arguments)
{
    AsyncArgumentsWithConstants result;
    result.reserve(arguments.size());
    for (const auto& argument : arguments)
    {
        const auto type = argument.getDataType();
        if (type.type == DataType::Type::UNDEFINED)
        {
            throw CannotInferSchema("Async argument type remains unresolved after inference");
        }
        if (const auto constant = argument.tryGetAs<ConstantValueLogicalFunction>())
        {
            auto value = RuntimeValue::fromLiteral(type, constant->get().getConstantValue());
            if (!value)
            {
                throw CannotInferSchema("Invalid async constant argument: {}", value.error().what());
            }
            result.emplace_back(type, std::move(*value));
        }
        else
        {
            result.emplace_back(type, std::nullopt);
        }
    }
    return result;
}
}
