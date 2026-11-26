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
#include <Operators/AsyncLoweringContext.hpp>

#include <algorithm>
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/RuntimeValue.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Sources/AsyncInputAccessor.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
AsyncLoweringContext::AsyncLoweringContext(std::unordered_map<QualifiedIdentifierBase<1>, AsyncInputAccessor> fields)
    : fields(std::move(fields))
{
}

AsyncInputAccessor AsyncLoweringContext::lower(const LogicalFunction& function)
{
    if (const auto constant = function.tryGetAs<ConstantValueLogicalFunction>())
    {
        auto value = RuntimeValue::fromLiteral(function.getDataType(), constant.value()->getConstantValue());
        if (!value)
        {
            throw std::move(value).error();
        }
        return AsyncInputAccessor{std::move(*value)};
    }
    const auto field = function.tryGetAs<FieldAccessLogicalFunction>();
    if (!field)
    {
        throw InvalidQuerySyntax("Async input expressions must be materialized before lowering");
    }
    const auto& name = field.value()->getField().getFullyQualifiedName();
    const auto found = fields.find(name);
    if (found == fields.end() || found->second.getDataType() != function.getDataType())
    {
        throw CannotInferSchema("Async input field {} is absent or has an incompatible type in its lowering context", name);
    }
    const auto& accessor = found->second;
    if (const auto producer = accessor.getProducer(); producer && std::ranges::find(dependencies, producer) == dependencies.end())
    {
        dependencies.push_back(producer);
    }
    return accessor;
}

std::optional<AsyncInputAccessor> AsyncLoweringContext::lower(const std::optional<LogicalFunction>& function)
{
    if (!function)
    {
        return std::nullopt;
    }
    return lower(*function);
}

const std::vector<std::shared_ptr<const AsyncCall>>& AsyncLoweringContext::getDependencies() const
{
    return dependencies;
}
}
