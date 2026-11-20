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

#include <concepts>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// The signature of a function of our engine that SQL does not have (e.g. FROM_BASE64, MEDIAN). The planner offers the
/// function to queries with it. The registry key is the name of the function in SQL.
struct FunctionSignature
{
    /// The families of the operands, as Calcite names them (SqlTypeFamily, e.g. STRING, NUMERIC).
    std::vector<std::string> operands;
    /// The result type, as Calcite names it (SqlTypeName, e.g. VARBINARY, DOUBLE). It is nullable if an operand is.
    std::string result;
    /// An aggregation (in the AggregationPhysicalFunctionRegistry), else a scalar function (in the PhysicalFunctionRegistry,
    /// under the lower-case name).
    bool aggregation;
};

/// The class of a function signature: `<Name>FunctionSignature` in `<Name>FunctionSignature.hpp`, registered with
/// `add_registry_entry(FunctionSignature <Name> KEY <SQL name>)`.
template <typename T>
concept FunctionSignatureConcept = requires {
    { T::OPERANDS } -> std::ranges::range;
    { T::RESULT } -> std::convertible_to<std::string_view>;
    { T::AGGREGATION } -> std::convertible_to<bool>;
};

template <FunctionSignatureConcept T>
FunctionSignature signatureOf()
{
    return FunctionSignature{
        .operands = T::OPERANDS | std::views::transform([](const auto& operand) { return std::string(operand); }) | std::ranges::to<std::vector>(),
        .result = std::string(T::RESULT),
        .aggregation = T::AGGREGATION};
}

class FunctionSignatureRegistry : public RuntimeRegistry<FunctionSignatureRegistry, std::string, FunctionSignature, /*CaseSensitive*/ false>
{
};

}
