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

#include <Functions/FtLogScoreLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
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

FtLogScoreLogicalFunction::FtLogScoreLogicalFunction(LogicalFunction ts, LogicalFunction slot, LogicalFunction score)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT32))
    , ts(std::move(ts))
    , slot(std::move(slot))
    , score(std::move(score))
{
}

FtLogScoreLogicalFunction::FtLogScoreLogicalFunction(const std::vector<LogicalFunction>& children)
    : FtLogScoreLogicalFunction(children[0], children[1], children[2])
{
    INVARIANT(children.size() == 3, "{} requires exactly 3 children, but got {}", NAME, children.size());
}

bool FtLogScoreLogicalFunction::operator==(const FtLogScoreLogicalFunction& rhs) const
{
    return ts == rhs.ts and slot == rhs.slot and score == rhs.score;
}

std::string FtLogScoreLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({}, {}, {})", NAME, ts.explain(verbosity), slot.explain(verbosity), score.explain(verbosity));
}

DataType FtLogScoreLogicalFunction::getDataType() const
{
    return dataType;
}

FtLogScoreLogicalFunction FtLogScoreLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction FtLogScoreLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto inferredTs = ts.withInferredDataType(schema);
    auto inferredSlot = slot.withInferredDataType(schema);
    auto inferredScore = score.withInferredDataType(schema);

    if (not inferredTs.getDataType().isType(DataType::Type::UINT64))
    {
        throw DifferentFieldTypeExpected("{} expects its ts argument to be UINT64, but got {}", NAME, inferredTs.getDataType());
    }
    if (not inferredSlot.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("{} expects its slot argument to be VARSIZED, but got {}", NAME, inferredSlot.getDataType());
    }
    if (not inferredScore.getDataType().isType(DataType::Type::FLOAT32))
    {
        throw DifferentFieldTypeExpected("{} expects its score argument to be FLOAT32, but got {}", NAME, inferredScore.getDataType());
    }

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::FLOAT32);
    newDataType.nullable = inferredScore.getDataType().nullable;
    return withDataType(newDataType).withChildren({inferredTs, inferredSlot, inferredScore});
}

std::vector<LogicalFunction> FtLogScoreLogicalFunction::getChildren() const
{
    return {ts, slot, score};
}

FtLogScoreLogicalFunction FtLogScoreLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    INVARIANT(children.size() == 3, "{} requires exactly 3 children, but got {}", NAME, children.size());
    auto copy = *this;
    copy.ts = children[0];
    copy.slot = children[1];
    copy.score = children[2];
    return copy;
}

std::string_view FtLogScoreLogicalFunction::getType() const
{
    return NAME;
}

Reflected
Reflector<FtLogScoreLogicalFunction>::operator()(const FtLogScoreLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedFtLogScoreLogicalFunction{.children = function.getChildren()});
}

FtLogScoreLogicalFunction
Unreflector<FtLogScoreLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [children] = context.unreflect<detail::ReflectedFtLogScoreLogicalFunction>(reflected);
    if (children.size() != 3)
    {
        throw CannotDeserialize("{} requires exactly 3 children, but got {}", FtLogScoreLogicalFunction::NAME, children.size());
    }
    return FtLogScoreLogicalFunction{children};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
LogicalFunctionRegistryReturnType FtLogScoreLogicalFunction::createFT_LOG_SCORE(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 3)
    {
        throw CannotDeserialize("{} requires exactly 3 arguments (ts, slot, score), but got {}", NAME, arguments.children.size());
    }
    return FtLogScoreLogicalFunction{arguments.children};
}

}
