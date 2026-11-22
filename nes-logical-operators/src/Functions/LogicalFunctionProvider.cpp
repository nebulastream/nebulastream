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

#include <Functions/LogicalFunctionProvider.hpp>

#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/AsyncCallLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <AsyncOperatorRegistry.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES::LogicalFunctionProvider
{
LogicalFunction provide(const std::string& functionName, std::vector<LogicalFunction> arguments)
{
    if (auto function = tryProvide(functionName, std::move(arguments)))
    {
        return *function;
    }
    throw FunctionNotImplemented("{} is not a registered function", functionName);
}

std::optional<LogicalFunction> tryProvide(const std::string& functionName, std::vector<LogicalFunction> arguments)
{
    const auto factory = LogicalFunctionRegistry::instance().find(functionName);
    const auto async = AsyncOperatorRegistry::instance().find(functionName);
    if (factory && async)
    {
        throw InvalidQuerySyntax("Function '{}' is ambiguous: registered as both scalar and async", functionName);
    }
    if (factory)
    {
        return (*factory)(LogicalFunctionRegistryArguments{.children = std::move(arguments)});
    }
    if (async)
    {
        return AsyncCallLogicalFunction{functionName, std::move(arguments)};
    }
    return std::nullopt;
}
}
