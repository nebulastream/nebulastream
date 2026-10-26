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

#include <Functions/SemanticMatchLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

SemanticMatchLogicalFunction::SemanticMatchLogicalFunction(std::string modelName, LogicalFunction left, LogicalFunction right)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::BOOLEAN))
    , modelName(std::move(modelName))
    , left(std::move(left))
    , right(std::move(right))
{
}

bool SemanticMatchLogicalFunction::operator==(const SemanticMatchLogicalFunction& rhs) const
{
    return modelName == rhs.modelName and left == rhs.left and right == rhs.right;
}

DataType SemanticMatchLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction SemanticMatchLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.left = left.withInferredDataType(schema);
    copy.right = right.withInferredDataType(schema);
    for (const auto& side : {copy.left, copy.right})
    {
        /// SEM_CALL, which this turns into, takes VARSIZED inputs only.
        if (not side.getDataType().isType(DataType::Type::VARSIZED))
        {
            throw DifferentFieldTypeExpected("SEM_MATCH of model '{}' expects VARSIZED arguments, but got {}", modelName, side);
        }
    }
    return copy;
}

std::vector<LogicalFunction> SemanticMatchLogicalFunction::getChildren() const
{
    return {left, right};
}

SemanticMatchLogicalFunction SemanticMatchLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "SEM_MATCH has two children, got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
}

std::string_view SemanticMatchLogicalFunction::getType() const /// NOLINT(readability-convert-member-functions-to-static)
{
    return NAME;
}

std::string SemanticMatchLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("SEM_MATCH({}, {}, {})", modelName, left.explain(verbosity), right.explain(verbosity));
}

Reflected
Reflector<SemanticMatchLogicalFunction>::operator()(const SemanticMatchLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedSemanticMatchLogicalFunction{
            .modelName = function.getModelName(), .left = function.getLeft(), .right = function.getRight()});
}

SemanticMatchLogicalFunction
Unreflector<SemanticMatchLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [modelName, left, right] = context.unreflect<detail::ReflectedSemanticMatchLogicalFunction>(reflected);
    return SemanticMatchLogicalFunction{std::move(modelName), std::move(left), std::move(right)};
}

LogicalFunctionRegistryReturnType SemanticMatchLogicalFunction::createSEM_MATCH(LogicalFunctionRegistryArguments arguments)
{
    /// The parser reads the bare model name as a field reference; it is the first argument, as in SEM_MAP and SEM_FILTER.
    if (arguments.children.size() != 3)
    {
        throw InvalidQuerySyntax("SEM_MATCH expects a model name and two arguments, but got {} arguments", arguments.children.size());
    }
    const auto model = arguments.children[0].tryGetAs<UnboundFieldAccessLogicalFunction>();
    if (not model.has_value())
    {
        throw InvalidQuerySyntax("SEM_MATCH expects a semantic model name as its first argument, but got {}", arguments.children[0]);
    }
    return SemanticMatchLogicalFunction{model.value()->getFieldName().asCanonicalString(), arguments.children[1], arguments.children[2]};
}

}
