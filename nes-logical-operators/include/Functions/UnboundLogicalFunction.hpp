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

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Parser-stage placeholder for an overloaded function call. The parser emits
/// `UnboundLogicalFunction("<op>", children)` for binary operators and
/// function calls; the actual overload is picked during type inference, when
/// children's logical types are known.
///
/// Resolution: `withInferredDataType` first infers all children, then
/// constructs a mangled overload key `<op>_<lhsType>_<rhsType>...` and looks
/// it up in `LogicalFunctionRegistry`. If no overload matches, falls back to
/// the bare `<op>` factory (the legacy non-overloaded entry). On hit, returns
/// the resolved function with `withInferredDataType` applied so its data type
/// stamp is set.
///
/// `UnboundLogicalFunction` should never reach the optimizer, the physical
/// lowering, or serialization — `TypeInferenceRule` replaces it. The
/// `Reflector` specialization throws to enforce this.
class UnboundLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Unbound";

    UnboundLogicalFunction(std::string operationName, std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const UnboundLogicalFunction& rhs) const;

    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] UnboundLogicalFunction withLogicalType(const LogicalType& logicalType) const;
    [[nodiscard]] LogicalFunction withInferredLogicalType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] UnboundLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const std::string& getOperationName() const { return operationName; }

private:
    std::string operationName;
    std::vector<LogicalFunction> children;

    friend struct Reflector<UnboundLogicalFunction>;
};

template <>
struct Reflector<UnboundLogicalFunction>
{
    Reflected operator()(const UnboundLogicalFunction& function) const;
};

static_assert(LogicalFunctionConcept<UnboundLogicalFunction>);

}

FMT_OSTREAM(NES::UnboundLogicalFunction);
