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
#include <PlanRewriteUtils.hpp>

#include <unordered_map>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>

namespace NES
{
LogicalFunction replaceFieldAccesses(const LogicalFunction& function, const std::unordered_map<Field, Field>& fields)
{
    if (const auto fieldAccessFunction = function.tryGetAs<FieldAccessLogicalFunction>())
    {
        auto field = fieldAccessFunction.value()->getField();

        if (const auto it = fields.find(field); it != fields.end())
        {
            return FieldAccessLogicalFunction{it->second};
        }
        return function;
    }

    std::vector<LogicalFunction> newChildren;
    for (const auto& child : function.getChildren())
    {
        newChildren.emplace_back(replaceFieldAccesses(child, fields));
    }

    return function.withChildren(newChildren);
}
}
