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

#include <Operators/Windows/Aggregations/AggregationLogicalFunctionProvider.hpp>

#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES::AggregationLogicalFunctionProvider
{

std::optional<Description> tryDescribe(const std::string& functionName)
{
    if (const auto entry = AggregationLogicalFunctionRegistry::instance().find(functionName))
    {
        return Description{.name = entry->name, .isStatistic = entry->isStatistic};
    }
    return std::nullopt;
}

std::optional<WindowAggregationLogicalFunction>
tryProvide(const std::string& functionName, AggregationLogicalFunctionRegistryArguments arguments)
{
    if (const auto entry = AggregationLogicalFunctionRegistry::instance().find(functionName))
    {
        return entry->create(std::move(arguments));
    }
    return std::nullopt;
}

WindowAggregationLogicalFunction provide(const std::string& functionName, AggregationLogicalFunctionRegistryArguments arguments)
{
    if (auto function = tryProvide(functionName, std::move(arguments)))
    {
        return *function;
    }
    throw UnknownAggregationType("{} is not a registered aggregation function", functionName);
}

std::vector<std::string> registeredNames()
{
    return AggregationLogicalFunctionRegistry::instance().getRegisteredNames();
}

}
