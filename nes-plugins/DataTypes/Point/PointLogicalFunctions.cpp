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

#include <PointLogicalFunctions.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

namespace
{
LogicalType pointType(DataType::NULLABLE nullable = DataType::NULLABLE::NOT_NULLABLE)
{
    return LogicalType{"Point", {}, nullable};
}

LogicalType float64Type(DataType::NULLABLE nullable = DataType::NULLABLE::NOT_NULLABLE)
{
    return LogicalType{"FLOAT64", {}, nullable};
}

LogicalType booleanType(DataType::NULLABLE nullable = DataType::NULLABLE::NOT_NULLABLE)
{
    return LogicalType{"BOOLEAN", {}, nullable};
}

DataType::NULLABLE joinNullable(const std::vector<LogicalFunction>& children)
{
    for (const auto& child : children)
    {
        if (child.getLogicalType().isNullable())
        {
            return DataType::NULLABLE::IS_NULLABLE;
        }
    }
    return DataType::NULLABLE::NOT_NULLABLE;
}

/// Compound logical types like Point have no primitive DataType representation.
/// Returning UNDEFINED keeps the legacy DataType bridge happy; the projection
/// flattening inspects `getLogicalType()` directly to look up the layout.
DataType compoundDataType(DataType::NULLABLE nullable)
{
    return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED, nullable);
}
}

/// ===== PointConstructLogicalFunction =====

PointConstructLogicalFunction::PointConstructLogicalFunction(std::vector<LogicalFunction> children)
    : logicalType(pointType()), children(std::move(children))
{
}

bool PointConstructLogicalFunction::operator==(const PointConstructLogicalFunction& rhs) const
{
    return children == rhs.children;
}

DataType PointConstructLogicalFunction::getDataType() const
{
    return compoundDataType(logicalType.getNullable());
}

LogicalType PointConstructLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointConstructLogicalFunction PointConstructLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = pointType(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointConstructLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto inferredChildren = children
        | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); }) | std::ranges::to<std::vector>();
    PointConstructLogicalFunction copy{inferredChildren};
    copy.logicalType = pointType(joinNullable(inferredChildren));
    return copy;
}

std::vector<LogicalFunction> PointConstructLogicalFunction::getChildren() const
{
    return children;
}

PointConstructLogicalFunction PointConstructLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    auto copy = *this;
    copy.children = newChildren;
    copy.logicalType = pointType(joinNullable(newChildren));
    return copy;
}

std::string_view PointConstructLogicalFunction::getType() const
{
    return NAME;
}

std::string PointConstructLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    auto explained = children | std::views::transform([&](const auto& c) { return c.explain(verbosity); });
    return fmt::format("Point({})", fmt::join(explained, ", "));
}

Reflected Reflector<PointConstructLogicalFunction>::operator()(const PointConstructLogicalFunction& function) const
{
    auto kids = function.children | std::views::transform([](const auto& c) { return std::optional<LogicalFunction>{c}; })
        | std::ranges::to<std::vector>();
    return reflect(detail::ReflectedPointFunction{.children = std::move(kids)});
}

PointConstructLogicalFunction Unreflector<PointConstructLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    std::vector<LogicalFunction> children;
    children.reserve(kids.size());
    for (auto& opt : kids)
    {
        if (!opt.has_value())
        {
            throw CannotDeserialize("Point function child is missing");
        }
        children.push_back(std::move(opt).value());
    }
    return PointConstructLogicalFunction{std::move(children)};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointConstructLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 3)
    {
        throw CannotDeserialize("Point requires exactly three children, but got {}", arguments.children.size());
    }
    return PointConstructLogicalFunction{std::move(arguments.children)};
}

/// ===== PointAddLogicalFunction =====

PointAddLogicalFunction::PointAddLogicalFunction(LogicalFunction left, LogicalFunction right)
    : logicalType(pointType()), left(std::move(left)), right(std::move(right))
{
}

bool PointAddLogicalFunction::operator==(const PointAddLogicalFunction& rhs) const
{
    return (left == rhs.left && right == rhs.right) || (left == rhs.right && right == rhs.left);
}

DataType PointAddLogicalFunction::getDataType() const
{
    return compoundDataType(logicalType.getNullable());
}

LogicalType PointAddLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointAddLogicalFunction PointAddLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = pointType(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointAddLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto l = left.withInferredDataType(schema);
    auto r = right.withInferredDataType(schema);
    PointAddLogicalFunction copy{l, r};
    copy.logicalType = pointType(joinNullable({l, r}));
    return copy;
}

std::vector<LogicalFunction> PointAddLogicalFunction::getChildren() const
{
    return {left, right};
}

PointAddLogicalFunction PointAddLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "PointAdd requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.left = newChildren[0];
    copy.right = newChildren[1];
    copy.logicalType = pointType(joinNullable(newChildren));
    return copy;
}

std::string_view PointAddLogicalFunction::getType() const
{
    return NAME;
}

std::string PointAddLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("PointAdd({}, {})", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<PointAddLogicalFunction>::operator()(const PointAddLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointFunction{.children = {function.left, function.right}});
}

PointAddLogicalFunction Unreflector<PointAddLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    if (kids.size() != 2 || !kids[0].has_value() || !kids[1].has_value())
    {
        throw CannotDeserialize("PointAdd requires exactly two children");
    }
    return PointAddLogicalFunction{kids[0].value(), kids[1].value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointAddLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointAddLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointAdd requires exactly two children, got {}", arguments.children.size());
    }
    return PointAddLogicalFunction{arguments.children[0], arguments.children[1]};
}

/// ===== PointSubLogicalFunction =====

PointSubLogicalFunction::PointSubLogicalFunction(LogicalFunction left, LogicalFunction right)
    : logicalType(pointType()), left(std::move(left)), right(std::move(right))
{
}

bool PointSubLogicalFunction::operator==(const PointSubLogicalFunction& rhs) const
{
    return left == rhs.left && right == rhs.right;
}

DataType PointSubLogicalFunction::getDataType() const
{
    return compoundDataType(logicalType.getNullable());
}

LogicalType PointSubLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointSubLogicalFunction PointSubLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = pointType(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointSubLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto l = left.withInferredDataType(schema);
    auto r = right.withInferredDataType(schema);
    PointSubLogicalFunction copy{l, r};
    copy.logicalType = pointType(joinNullable({l, r}));
    return copy;
}

std::vector<LogicalFunction> PointSubLogicalFunction::getChildren() const
{
    return {left, right};
}

PointSubLogicalFunction PointSubLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "PointSub requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.left = newChildren[0];
    copy.right = newChildren[1];
    copy.logicalType = pointType(joinNullable(newChildren));
    return copy;
}

std::string_view PointSubLogicalFunction::getType() const
{
    return NAME;
}

std::string PointSubLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("PointSub({}, {})", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<PointSubLogicalFunction>::operator()(const PointSubLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointFunction{.children = {function.left, function.right}});
}

PointSubLogicalFunction Unreflector<PointSubLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    if (kids.size() != 2 || !kids[0].has_value() || !kids[1].has_value())
    {
        throw CannotDeserialize("PointSub requires exactly two children");
    }
    return PointSubLogicalFunction{kids[0].value(), kids[1].value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointSubLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointSubLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointSub requires exactly two children, got {}", arguments.children.size());
    }
    return PointSubLogicalFunction{arguments.children[0], arguments.children[1]};
}

/// ===== PointScaleLogicalFunction =====

PointScaleLogicalFunction::PointScaleLogicalFunction(LogicalFunction point, LogicalFunction scalar)
    : logicalType(pointType()), point(std::move(point)), scalar(std::move(scalar))
{
}

bool PointScaleLogicalFunction::operator==(const PointScaleLogicalFunction& rhs) const
{
    return point == rhs.point && scalar == rhs.scalar;
}

DataType PointScaleLogicalFunction::getDataType() const
{
    return compoundDataType(logicalType.getNullable());
}

LogicalType PointScaleLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointScaleLogicalFunction PointScaleLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = pointType(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointScaleLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto p = point.withInferredDataType(schema);
    auto s = scalar.withInferredDataType(schema);
    PointScaleLogicalFunction copy{p, s};
    copy.logicalType = pointType(joinNullable({p, s}));
    return copy;
}

std::vector<LogicalFunction> PointScaleLogicalFunction::getChildren() const
{
    return {point, scalar};
}

PointScaleLogicalFunction PointScaleLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "PointScale requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.point = newChildren[0];
    copy.scalar = newChildren[1];
    copy.logicalType = pointType(joinNullable(newChildren));
    return copy;
}

std::string_view PointScaleLogicalFunction::getType() const
{
    return NAME;
}

std::string PointScaleLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("PointScale({}, {})", point.explain(verbosity), scalar.explain(verbosity));
}

Reflected Reflector<PointScaleLogicalFunction>::operator()(const PointScaleLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointFunction{.children = {function.point, function.scalar}});
}

PointScaleLogicalFunction Unreflector<PointScaleLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    if (kids.size() != 2 || !kids[0].has_value() || !kids[1].has_value())
    {
        throw CannotDeserialize("PointScale requires exactly two children");
    }
    return PointScaleLogicalFunction{kids[0].value(), kids[1].value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointScaleLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointScaleLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointScale requires exactly two children, got {}", arguments.children.size());
    }
    return PointScaleLogicalFunction{arguments.children[0], arguments.children[1]};
}

/// ===== PointDistanceLogicalFunction =====

PointDistanceLogicalFunction::PointDistanceLogicalFunction(LogicalFunction left, LogicalFunction right)
    : logicalType(float64Type()), left(std::move(left)), right(std::move(right))
{
}

bool PointDistanceLogicalFunction::operator==(const PointDistanceLogicalFunction& rhs) const
{
    return (left == rhs.left && right == rhs.right) || (left == rhs.right && right == rhs.left);
}

DataType PointDistanceLogicalFunction::getDataType() const
{
    return DataTypeProvider::provideDataType(DataType::Type::FLOAT64, logicalType.getNullable());
}

LogicalType PointDistanceLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointDistanceLogicalFunction PointDistanceLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = float64Type(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointDistanceLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto l = left.withInferredDataType(schema);
    auto r = right.withInferredDataType(schema);
    PointDistanceLogicalFunction copy{l, r};
    copy.logicalType = float64Type(joinNullable({l, r}));
    return copy;
}

std::vector<LogicalFunction> PointDistanceLogicalFunction::getChildren() const
{
    return {left, right};
}

PointDistanceLogicalFunction PointDistanceLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "PointDistance requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.left = newChildren[0];
    copy.right = newChildren[1];
    copy.logicalType = float64Type(joinNullable(newChildren));
    return copy;
}

std::string_view PointDistanceLogicalFunction::getType() const
{
    return NAME;
}

std::string PointDistanceLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("PointDistance({}, {})", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<PointDistanceLogicalFunction>::operator()(const PointDistanceLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointFunction{.children = {function.left, function.right}});
}

PointDistanceLogicalFunction Unreflector<PointDistanceLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    if (kids.size() != 2 || !kids[0].has_value() || !kids[1].has_value())
    {
        throw CannotDeserialize("PointDistance requires exactly two children");
    }
    return PointDistanceLogicalFunction{kids[0].value(), kids[1].value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointDistanceLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointDistanceLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointDistance requires exactly two children, got {}", arguments.children.size());
    }
    return PointDistanceLogicalFunction{arguments.children[0], arguments.children[1]};
}

/// ===== PointEqualsLogicalFunction =====

PointEqualsLogicalFunction::PointEqualsLogicalFunction(LogicalFunction left, LogicalFunction right)
    : logicalType(booleanType()), left(std::move(left)), right(std::move(right))
{
}

bool PointEqualsLogicalFunction::operator==(const PointEqualsLogicalFunction& rhs) const
{
    return (left == rhs.left && right == rhs.right) || (left == rhs.right && right == rhs.left);
}

DataType PointEqualsLogicalFunction::getDataType() const
{
    return DataTypeProvider::provideDataType(DataType::Type::BOOLEAN, logicalType.getNullable());
}

LogicalType PointEqualsLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointEqualsLogicalFunction PointEqualsLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.logicalType = booleanType(dataType.nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
    return copy;
}

LogicalFunction PointEqualsLogicalFunction::withInferredDataType(const Schema& schema) const
{
    auto l = left.withInferredDataType(schema);
    auto r = right.withInferredDataType(schema);
    PointEqualsLogicalFunction copy{l, r};
    copy.logicalType = booleanType(joinNullable({l, r}));
    return copy;
}

std::vector<LogicalFunction> PointEqualsLogicalFunction::getChildren() const
{
    return {left, right};
}

PointEqualsLogicalFunction PointEqualsLogicalFunction::withChildren(const std::vector<LogicalFunction>& newChildren) const
{
    PRECONDITION(newChildren.size() == 2, "PointEquals requires exactly two children, got {}", newChildren.size());
    auto copy = *this;
    copy.left = newChildren[0];
    copy.right = newChildren[1];
    copy.logicalType = booleanType(joinNullable(newChildren));
    return copy;
}

std::string_view PointEqualsLogicalFunction::getType() const
{
    return NAME;
}

std::string PointEqualsLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("PointEquals({}, {})", left.explain(verbosity), right.explain(verbosity));
}

Reflected Reflector<PointEqualsLogicalFunction>::operator()(const PointEqualsLogicalFunction& function) const
{
    return reflect(detail::ReflectedPointFunction{.children = {function.left, function.right}});
}

PointEqualsLogicalFunction Unreflector<PointEqualsLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto [kids] = unreflect<detail::ReflectedPointFunction>(reflected);
    if (kids.size() != 2 || !kids[0].has_value() || !kids[1].has_value())
    {
        throw CannotDeserialize("PointEquals requires exactly two children");
    }
    return PointEqualsLogicalFunction{kids[0].value(), kids[1].value()};
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterPointEqualsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<PointEqualsLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("PointEquals requires exactly two children, got {}", arguments.children.size());
    }
    return PointEqualsLogicalFunction{arguments.children[0], arguments.children[1]};
}

/// Mangled-name overload entry: `Equals_Point_Point` is the key built by
/// `UnboundLogicalFunction::withInferredDataType` when both children's logical
/// type names equal "Point". Returns the same class as the bare PointEquals
/// factory; the separate plugin name is what makes it discoverable via the
/// overload-mangling path.
LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEquals_Point_PointLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return RegisterPointEqualsLogicalFunction(std::move(arguments));
}

}
