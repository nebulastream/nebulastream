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
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{
struct OperatorFunctionRegistryEntry
{
    Identifier resultName;
    std::function<DataType(const std::vector<LogicalFunction>&)> inferResultType;
    std::function<LogicalOperator(LogicalOperator, const std::vector<LogicalFunction>&)> createOperator;
    std::function<std::optional<std::vector<LogicalFunction>>(const LogicalOperator&)> getArgumentsFromOperator{};
};

class OperatorFunctionRegistry final : public RuntimeRegistry<OperatorFunctionRegistry, std::string, OperatorFunctionRegistryEntry, false>
{
public:
    static OperatorFunctionRegistry& instance();
};

[[nodiscard]] std::optional<std::pair<std::string, std::vector<LogicalFunction>>> findOperatorFunction(const LogicalOperator& op);
}
