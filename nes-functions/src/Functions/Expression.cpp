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


#include <Functions/Expression.hpp>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <fmt/std.h>

namespace NES
{
void ExpressionValue::inferStamp(const Schema& schema)
{
    expression->inferStamp(schema, children);
}
std::optional<DataType> ExpressionValue::getStamp() const
{
    return expression->getStamp();
}
bool ExpressionValue::operator==(ExpressionValue other) const
{
    return children == other.children && expression->equals(*other.expression);
}
const std::vector<ExpressionValue>& ExpressionValue::getChildren() const
{
    return children;
}
ExpressionValue::ExpressionValue(COW<Expression> expression, std::vector<ExpressionValue> children)
    : expression(std::move(expression)), children(std::move(children))
{
}
coro::generator<const ExpressionValue&> ExpressionValue::dfs() const
{
    co_yield *this;
    for (const auto& child : children)
    {
        for (const auto& c : child.dfs())
        {
            co_yield c;
        }
    }
}
auto format_as(const ExpressionValue& value) -> std::string
{
    if (!value.children.empty())
    {
        return fmt::format("{}({}) -> {}", value.expression->toString(), fmt::join(value.children, ", "), value.getStamp());
    }
    else
    {
        return fmt::format("{} -> {}", value.expression->toString(), value.getStamp());
    }
}
}
