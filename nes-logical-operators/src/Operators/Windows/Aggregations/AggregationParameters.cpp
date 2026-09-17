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

#include <Operators/Windows/Aggregations/AggregationParameters.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

AggregationFieldAccess parseFieldParameter(const LogicalFunction& parameter, const std::string_view description)
{
    if (const auto unbound = parameter.tryGetAs<UnboundFieldAccessLogicalFunction>())
    {
        return unbound.value();
    }
    if (const auto bound = parameter.tryGetAs<FieldAccessLogicalFunction>())
    {
        return bound.value();
    }
    throw InvalidQuerySyntax("Expected a field name for {}, but got {}", description, parameter);
}

uint64_t parseUnsignedParameter(const LogicalFunction& parameter, const std::string_view description)
{
    const auto constant = parameter.tryGetAs<ConstantValueLogicalFunction>();
    if (not constant)
    {
        throw InvalidQuerySyntax("Expected an unsigned integer constant for {}, but got {}", description, parameter);
    }
    const auto parsed = from_chars<uint64_t>(constant.value()->getConstantValue());
    if (not parsed.has_value())
    {
        throw InvalidQuerySyntax(
            "Expected an unsigned integer constant for {}, but got {}", description, constant.value()->getConstantValue());
    }
    return parsed.value();
}

std::string parseStringParameter(const LogicalFunction& parameter, const std::string_view description)
{
    const auto constant = parameter.tryGetAs<ConstantValueLogicalFunction>();
    if (not constant)
    {
        throw InvalidQuerySyntax("Expected a quoted string constant for {}, but got {}", description, parameter);
    }
    const auto raw = constant.value()->getConstantValue();
    if (raw.size() < 2 or raw.front() != '\'' or raw.back() != '\'')
    {
        throw InvalidQuerySyntax("Expected a quoted string constant for {}, but got {}", description, raw);
    }
    return raw.substr(1, raw.size() - 2);
}

}
