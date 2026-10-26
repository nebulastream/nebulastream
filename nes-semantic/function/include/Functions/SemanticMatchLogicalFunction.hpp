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

/// `SEM_MATCH(model, left, right)`: whether a semantic filter model with two INPUT fields judges the pair a match. Only
/// valid as a conjunct of an inner join's condition, which is how SEM_JOIN is written:
///
///     SELECT ... FROM a INNER JOIN b ON SEM_MATCH(model, a.x, b.y) [AND ...] WINDOW ...
///
/// A placeholder that is never executed: `SemanticCallTranslationRule` takes it out of the join condition and puts a
/// selection on a `SEM_CALL` verdict above the join instead. It is BOOLEAN, so the join's type inference accepts it.
class SemanticMatchLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "SemanticMatch";

    SemanticMatchLogicalFunction(std::string modelName, LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const SemanticMatchLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema<Field, Unordered>& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] SemanticMatchLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const std::string& getModelName() const { return modelName; }

    [[nodiscard]] const LogicalFunction& getLeft() const { return left; }

    [[nodiscard]] const LogicalFunction& getRight() const { return right; }

    static LogicalFunctionRegistryReturnType createSEM_MATCH(LogicalFunctionRegistryArguments arguments);

private:
    DataType dataType;
    std::string modelName;
    LogicalFunction left;
    LogicalFunction right;
};

namespace detail
{
struct ReflectedSemanticMatchLogicalFunction
{
    std::string modelName;
    LogicalFunction left;
    LogicalFunction right;
};
}

template <>
struct Unreflector<SemanticMatchLogicalFunction>
{
    SemanticMatchLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

template <>
struct Reflector<SemanticMatchLogicalFunction>
{
    Reflected operator()(const SemanticMatchLogicalFunction& function, const ReflectionContext& context) const;
};

static_assert(LogicalFunctionConcept<SemanticMatchLogicalFunction>);

}

FMT_OSTREAM(NES::SemanticMatchLogicalFunction);
