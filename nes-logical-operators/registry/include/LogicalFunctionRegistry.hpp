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

#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <Functions/LogicalFunction.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Types/NESType.hpp>
#include <Util/RuntimeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

using LogicalFunctionRegistryReturnType = LogicalFunction;

struct LogicalFunctionRegistryArguments
{
    std::vector<LogicalFunction> children;
};

using LogicalFunctionFn = std::function<LogicalFunctionRegistryReturnType(LogicalFunctionRegistryArguments)>;

/// Trait attached to `NESType`s representing scalar `LogicalFunction` expressions.
/// When instantiated at the type level over concrete argument types, `resultScalarType`
/// records the inferred output `NESType` (e.g. for `ClosedBinaryFunction("Add")`).
struct LogicalFunctionTypeTrait
{
    std::optional<NESType> resultScalarType;
};

/// Converts any scalar literal or `LogicalFunction` `NESValue` into a `LogicalFunction` AST node.
[[nodiscard]] std::expected<LogicalFunction, Exception> nesValueToLogicalFunction(const NESValue& value);

/// Wraps a `LogicalFunction` AST node into a `NESValue` carrying `LogicalFunctionTypeTrait`.
[[nodiscard]] NESValue makeLogicalFunctionValue(LogicalFunction function);

class LogicalFunctionRegistry : public RuntimeRegistry<LogicalFunctionRegistry, std::string, LogicalFunctionFn, /*CaseSensitive*/ false>
{
public:
    static constexpr bool EXPOSE_TO_SQL_SCOPE = true;

    static LogicalFunctionRegistry& instance();
    static NESTypeClass logicalFunctionTypeClass();
    static NESTypeClass closedBinaryFunctionTypeClass(QualifiedIdentifier functionName);
    static std::vector<NamedNESParametrizedTypeClass> getParametrizedTypeClasses();
};

NESFunctor makeLogicalFunctionFunctor(std::string_view key, LogicalFunctionFn factory);

}
