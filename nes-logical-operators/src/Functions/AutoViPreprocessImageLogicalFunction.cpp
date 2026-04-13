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

#include <Functions/AutoViPreprocessImageLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

AutoViPreprocessImageLogicalFunction::AutoViPreprocessImageLogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool AutoViPreprocessImageLogicalFunction::operator==(const AutoViPreprocessImageLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string AutoViPreprocessImageLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, child.explain(verbosity));
}

DataType AutoViPreprocessImageLogicalFunction::getDataType() const
{
    return dataType;
}

AutoViPreprocessImageLogicalFunction AutoViPreprocessImageLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction AutoViPreprocessImageLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto newChild = child.withInferredDataType(schema);
    if (not newChild.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("{} expects a VARSIZED input, but got {}", NAME, newChild.getDataType());
    }
    const auto nullable = newChild.getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    return withDataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED, nullable)).withChildren({newChild});
}

std::vector<LogicalFunction> AutoViPreprocessImageLogicalFunction::getChildren() const
{
    return {child};
}

AutoViPreprocessImageLogicalFunction
AutoViPreprocessImageLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "{} requires exactly one child, but got {}", NAME, children.size());
    auto copy = *this;
    copy.child = children[0];
    const auto nullable = children[0].getDataType().nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED, nullable);
    return copy;
}

std::string_view AutoViPreprocessImageLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<AutoViPreprocessImageLogicalFunction>::operator()(const AutoViPreprocessImageLogicalFunction& function) const
{
    return reflect(detail::ReflectedAutoViPreprocessImageLogicalFunction{.child = function.child});
}

AutoViPreprocessImageLogicalFunction Unreflector<AutoViPreprocessImageLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedAutoViPreprocessImageLogicalFunction>(reflected);
    if (!child.has_value())
    {
        throw CannotDeserialize("AutoViPreprocessImageLogicalFunction is missing its child");
    }
    return AutoViPreprocessImageLogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterAUTOVI_PREPROCESS_IMAGELogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<AutoViPreprocessImageLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("{} requires one argument", AutoViPreprocessImageLogicalFunction::NAME);
    }
    return AutoViPreprocessImageLogicalFunction(arguments.children.back());
}

}
