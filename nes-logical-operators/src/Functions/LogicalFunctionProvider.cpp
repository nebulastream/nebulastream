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
#include <Functions/LogicalFunction.hpp>
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
    return LogicalFunctionRegistry::instance().create(
        functionName, LogicalFunctionRegistryArguments{.config = {}, .children = std::move(arguments), .dataType = {}});
}
}
