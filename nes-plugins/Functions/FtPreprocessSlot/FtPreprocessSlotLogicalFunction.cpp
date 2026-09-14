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

#include <Functions/FtPreprocessSlotLogicalFunction.hpp>

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

FtPreprocessSlotLogicalFunction::FtPreprocessSlotLogicalFunction(
    LogicalFunction image, LogicalFunction x, LogicalFunction y, LogicalFunction w, LogicalFunction h)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
    , image(std::move(image))
    , x(std::move(x))
    , y(std::move(y))
    , w(std::move(w))
    , h(std::move(h))
{
}

FtPreprocessSlotLogicalFunction::FtPreprocessSlotLogicalFunction(const std::vector<LogicalFunction>& children)
    : FtPreprocessSlotLogicalFunction(children[0], children[1], children[2], children[3], children[4])
{
    INVARIANT(children.size() == 5, "{} requires exactly 5 children, but got {}", NAME, children.size());
}

bool FtPreprocessSlotLogicalFunction::operator==(const FtPreprocessSlotLogicalFunction& rhs) const
{
    return image == rhs.image and x == rhs.x and y == rhs.y and w == rhs.w and h == rhs.h;
}

std::string FtPreprocessSlotLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "{}({}, {}, {}, {}, {})",
        NAME,
        image.explain(verbosity),
        x.explain(verbosity),
        y.explain(verbosity),
        w.explain(verbosity),
        h.explain(verbosity));
}

DataType FtPreprocessSlotLogicalFunction::getDataType() const
{
    return dataType;
}

FtPreprocessSlotLogicalFunction FtPreprocessSlotLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction FtPreprocessSlotLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto inferredImage = image.withInferredDataType(schema);
    auto inferredX = x.withInferredDataType(schema);
    auto inferredY = y.withInferredDataType(schema);
    auto inferredW = w.withInferredDataType(schema);
    auto inferredH = h.withInferredDataType(schema);

    if (not inferredImage.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("{} expects a VARSIZED image argument, but got {}", NAME, inferredImage.getDataType());
    }
    for (const auto& [name, roi] : {std::pair{"x", inferredX}, std::pair{"y", inferredY}, std::pair{"w", inferredW}, std::pair{"h", inferredH}})
    {
        if (not roi.getDataType().isType(DataType::Type::UINT32))
        {
            throw DifferentFieldTypeExpected(
                "{} expects its {} argument to be UINT32 (cast the slot ROI literal explicitly), but got {}",
                NAME,
                name,
                roi.getDataType());
        }
    }

    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = inferredImage.getDataType().nullable;
    return withDataType(newDataType).withChildren({inferredImage, inferredX, inferredY, inferredW, inferredH});
}

std::vector<LogicalFunction> FtPreprocessSlotLogicalFunction::getChildren() const
{
    return {image, x, y, w, h};
}

FtPreprocessSlotLogicalFunction FtPreprocessSlotLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    INVARIANT(children.size() == 5, "{} requires exactly 5 children, but got {}", NAME, children.size());
    auto copy = *this;
    copy.image = children[0];
    copy.x = children[1];
    copy.y = children[2];
    copy.w = children[3];
    copy.h = children[4];
    return copy;
}

std::string_view FtPreprocessSlotLogicalFunction::getType() const
{
    return NAME;
}

Reflected
Reflector<FtPreprocessSlotLogicalFunction>::operator()(const FtPreprocessSlotLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedFtPreprocessSlotLogicalFunction{.children = function.getChildren()});
}

FtPreprocessSlotLogicalFunction
Unreflector<FtPreprocessSlotLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [children] = context.unreflect<detail::ReflectedFtPreprocessSlotLogicalFunction>(reflected);
    if (children.size() != 5)
    {
        throw CannotDeserialize("{} requires exactly 5 children, but got {}", FtPreprocessSlotLogicalFunction::NAME, children.size());
    }
    return FtPreprocessSlotLogicalFunction{children};
}

/// NOLINTNEXTLINE(readability-identifier-naming)
LogicalFunctionRegistryReturnType FtPreprocessSlotLogicalFunction::createFT_PREPROCESS_SLOT(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 5)
    {
        throw CannotDeserialize(
            "{} requires exactly 5 arguments (image, x, y, w, h), but got {}", NAME, arguments.children.size());
    }
    return FtPreprocessSlotLogicalFunction{arguments.children};
}

}
