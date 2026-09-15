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

#include <Functions/FtPreprocessAllSlotsLogicalFunction.hpp>

#include <string>
#include <string_view>
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

FtPreprocessAllSlotsLogicalFunction::FtPreprocessAllSlotsLogicalFunction(LogicalFunction image)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), image(std::move(image))
{
}

FtPreprocessAllSlotsLogicalFunction::FtPreprocessAllSlotsLogicalFunction(const std::vector<LogicalFunction>& children)
    : FtPreprocessAllSlotsLogicalFunction(children[0])
{
    INVARIANT(children.size() == 1, "{} requires exactly 1 child, but got {}", NAME, children.size());
}

bool FtPreprocessAllSlotsLogicalFunction::operator==(const FtPreprocessAllSlotsLogicalFunction& rhs) const
{
    return image == rhs.image;
}

std::string FtPreprocessAllSlotsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, image.explain(verbosity));
}

DataType FtPreprocessAllSlotsLogicalFunction::getDataType() const
{
    return dataType;
}

FtPreprocessAllSlotsLogicalFunction FtPreprocessAllSlotsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction FtPreprocessAllSlotsLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto inferredImage = image.withInferredDataType(schema);

    if (not inferredImage.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("{} expects a VARSIZED image argument, but got {}", NAME, inferredImage.getDataType());
    }

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = inferredImage.getDataType().nullable;
    return withDataType(newDataType).withChildren({inferredImage});
}

std::vector<LogicalFunction> FtPreprocessAllSlotsLogicalFunction::getChildren() const
{
    return {image};
}

FtPreprocessAllSlotsLogicalFunction FtPreprocessAllSlotsLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    INVARIANT(children.size() == 1, "{} requires exactly 1 child, but got {}", NAME, children.size());
    auto copy = *this;
    copy.image = children[0];
    return copy;
}

std::string_view FtPreprocessAllSlotsLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<FtPreprocessAllSlotsLogicalFunction>::operator()(
    const FtPreprocessAllSlotsLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedFtPreprocessAllSlotsLogicalFunction{.children = function.getChildren()});
}

FtPreprocessAllSlotsLogicalFunction
Unreflector<FtPreprocessAllSlotsLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [children] = context.unreflect<detail::ReflectedFtPreprocessAllSlotsLogicalFunction>(reflected);
    if (children.size() != 1)
    {
        throw CannotDeserialize("{} requires exactly 1 child, but got {}", FtPreprocessAllSlotsLogicalFunction::NAME, children.size());
    }
    return FtPreprocessAllSlotsLogicalFunction{children};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
LogicalFunctionRegistryReturnType FtPreprocessAllSlotsLogicalFunction::createFT_PREPROCESS_ALL_SLOTS(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("{} requires exactly 1 argument (image), but got {}", NAME, arguments.children.size());
    }
    return FtPreprocessAllSlotsLogicalFunction{arguments.children};
}

}
