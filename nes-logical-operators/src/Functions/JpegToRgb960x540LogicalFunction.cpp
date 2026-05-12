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

#include <Functions/JpegToRgb960x540LogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp> /// NOLINT(misc-include-cleaner)
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h> /// NOLINT(misc-include-cleaner)

namespace NES
{

/// NOLINTNEXTLINE(modernize-pass-by-value)
JpegToRgb960x540LogicalFunction::JpegToRgb960x540LogicalFunction(const LogicalFunction& child)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)), child(child)
{
}

bool JpegToRgb960x540LogicalFunction::operator==(const JpegToRgb960x540LogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string JpegToRgb960x540LogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("JPEG_TO_RGB_960X540({})", child.explain(verbosity));
}

DataType JpegToRgb960x540LogicalFunction::getDataType() const
{
    return dataType;
}

JpegToRgb960x540LogicalFunction JpegToRgb960x540LogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction JpegToRgb960x540LogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& chr : getChildren())
    {
        newChildren.push_back(chr.withInferredDataType(schema));
    }
    INVARIANT(newChildren.size() == 1, "JpegToRgb960x540LogicalFunction expects exactly one child but has {}", newChildren.size());
    if (not newChildren[0].getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("JPEG_TO_RGB_960X540 expects a VARSIZED input but got {}", newChildren[0].getDataType());
    }
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = newChildren[0].getDataType().nullable;
    return withDataType(newDataType).withChildren(newChildren);
}

std::vector<LogicalFunction> JpegToRgb960x540LogicalFunction::getChildren() const
{
    return {child};
}

JpegToRgb960x540LogicalFunction JpegToRgb960x540LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.child = children[0];
    return copy;
}

std::string_view JpegToRgb960x540LogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<JpegToRgb960x540LogicalFunction>::operator()(const JpegToRgb960x540LogicalFunction& function) const
{
    return reflect(detail::ReflectedJpegToRgb960x540LogicalFunction{.child = function.child});
}

JpegToRgb960x540LogicalFunction Unreflector<JpegToRgb960x540LogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [child] = unreflect<detail::ReflectedJpegToRgb960x540LogicalFunction>(reflected);

    if (!child.has_value())
    {
        throw CannotDeserialize("JpegToRgb960x540LogicalFunction is missing its child");
    }
    return JpegToRgb960x540LogicalFunction{child.value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterJPEG_TO_RGB_960X540LogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<JpegToRgb960x540LogicalFunction>(arguments.reflected);
    }
    if (arguments.children.empty())
    {
        throw CannotDeserialize("JPEG_TO_RGB_960X540 requires one argument");
    }
    return JpegToRgb960x540LogicalFunction(arguments.children.back());
}

}
