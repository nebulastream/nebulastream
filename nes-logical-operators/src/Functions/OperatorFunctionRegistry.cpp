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
#include <OperatorFunctionRegistry.hpp>

#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Operators/LogicalOperatorFwd.hpp>

namespace NES
{
OperatorFunctionRegistry& OperatorFunctionRegistry::instance()
{
    static OperatorFunctionRegistry registry;
    return registry;
}

std::optional<std::pair<std::string, std::vector<LogicalFunction>>> findOperatorFunction(const LogicalOperator& op)
{
    auto& registry = OperatorFunctionRegistry::instance();
    for (const auto& name : registry.getRegisteredNames())
    {
        const auto entry = registry.find(name);
        if (entry && entry->getArgumentsFromOperator)
        {
            if (auto arguments = entry->getArgumentsFromOperator(op))
            {
                return std::pair<std::string, std::vector<LogicalFunction>>{name, std::move(*arguments)};
            }
        }
    }
    return std::nullopt;
}
}
