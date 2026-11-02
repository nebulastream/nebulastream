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

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/AsyncLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

/// `SEM_CALL(payload, in1, ..., inN)`: one prompt to a semantic model for one row, answered with one value per step of
/// the model, a MAP step's answer or a FILTER step's verdict `'true'`/`'false'`. The call evaluates to the value of
/// step 0; `ASYNC_OUTPUT(call, k)` reads step k. Asynchronous only; there is no Nautilus implementation.
///
/// Not meant to be written by hand: `SemanticCallTranslationRule` emits it for SEM_MAP and SEM_FILTER. Its first
/// argument is the constant `SemanticCallPayload` (the model and its steps, as JSON); it is not a child, so the
/// lifting rule passes only the input fields to the call. `explain` names the model and a hash of the payload, since
/// calls that explain alike are evaluated once.
class SemanticCallLogicalFunction final : public AsyncLogicalFunction
{
public:
    static constexpr std::string_view NAME = "SemanticCall";

    SemanticCallLogicalFunction(std::string payload, std::vector<LogicalFunction> inputs);

    [[nodiscard]] bool operator==(const SemanticCallLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] SemanticCallLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    /// The encoded `SemanticCallPayload`.
    [[nodiscard]] std::string getAsyncConfiguration() const override { return payload; }

    /// One value per step: map outputs and filter verdicts, in step order.
    [[nodiscard]] size_t getNumberOfAsyncOutputs() const override { return numberOfSteps; }

    /// SEM_SCAN and SEM_WINDOW keep state per key.
    [[nodiscard]] bool dependsOnEarlierRows() const override { return keyed; }

    [[nodiscard]] const std::string& getPayload() const { return payload; }

    static LogicalFunctionRegistryReturnType createSEM_CALL(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::string payload;
    std::vector<LogicalFunction> inputs;
    /// Decoded from the payload once, for explain and the number of outputs.
    std::string model;
    size_t numberOfSteps;
    bool keyed = false;
};

namespace detail
{
struct ReflectedSemanticCallLogicalFunction
{
    std::string payload;
    std::vector<LogicalFunction> inputs;
};
}

template <>
struct Unreflector<SemanticCallLogicalFunction>
{
    SemanticCallLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<SemanticCallLogicalFunction>
{
    Reflected operator()(const SemanticCallLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<SemanticCallLogicalFunction>);

}

FMT_OSTREAM(NES::SemanticCallLogicalFunction);
