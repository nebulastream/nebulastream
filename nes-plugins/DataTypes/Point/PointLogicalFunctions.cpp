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
LogicalType pointType(Nullable nullable = Nullable::NOT_NULLABLE)
{
    return LogicalType{"Point", {}, nullable};
}

LogicalType floatType(Nullable nullable = Nullable::NOT_NULLABLE)
{
    return LogicalType{"FLOAT", {}, nullable};
}

LogicalType boolType(Nullable nullable = Nullable::NOT_NULLABLE)
{
    return LogicalType{"BOOL", {}, nullable};
}

Nullable joinNullable(const std::vector<LogicalFunction>& children)
{
    for (const auto& child : children)
    {
        if (child.getLogicalType().isNullable())
        {
            return Nullable::IS_NULLABLE;
        }
    }
    return Nullable::NOT_NULLABLE;
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

LogicalType PointConstructLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointConstructLogicalFunction PointConstructLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointConstructLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto inferredChildren = children
        | std::views::transform([&schema](const auto& child) { return child.withInferredLogicalType(schema); }) | std::ranges::to<std::vector>();
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

LogicalType PointAddLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointAddLogicalFunction PointAddLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointAddLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto l = left.withInferredLogicalType(schema);
    auto r = right.withInferredLogicalType(schema);
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

LogicalType PointSubLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointSubLogicalFunction PointSubLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointSubLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto l = left.withInferredLogicalType(schema);
    auto r = right.withInferredLogicalType(schema);
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

LogicalType PointScaleLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointScaleLogicalFunction PointScaleLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointScaleLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto p = point.withInferredLogicalType(schema);
    auto s = scalar.withInferredLogicalType(schema);
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
    : logicalType(floatType()), left(std::move(left)), right(std::move(right))
{
}

bool PointDistanceLogicalFunction::operator==(const PointDistanceLogicalFunction& rhs) const
{
    return (left == rhs.left && right == rhs.right) || (left == rhs.right && right == rhs.left);
}

LogicalType PointDistanceLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointDistanceLogicalFunction PointDistanceLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointDistanceLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto l = left.withInferredLogicalType(schema);
    auto r = right.withInferredLogicalType(schema);
    PointDistanceLogicalFunction copy{l, r};
    copy.logicalType = floatType(joinNullable({l, r}));
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
    copy.logicalType = floatType(joinNullable(newChildren));
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
    : logicalType(boolType()), left(std::move(left)), right(std::move(right))
{
}

bool PointEqualsLogicalFunction::operator==(const PointEqualsLogicalFunction& rhs) const
{
    return (left == rhs.left && right == rhs.right) || (left == rhs.right && right == rhs.left);
}

LogicalType PointEqualsLogicalFunction::getLogicalType() const
{
    return logicalType;
}

PointEqualsLogicalFunction PointEqualsLogicalFunction::withLogicalType(const LogicalType& newType) const
{
    auto copy = *this;
    copy.logicalType = newType;
    return copy;
}

LogicalFunction PointEqualsLogicalFunction::withInferredLogicalType(const Schema& schema) const
{
    auto l = left.withInferredLogicalType(schema);
    auto r = right.withInferredLogicalType(schema);
    PointEqualsLogicalFunction copy{l, r};
    copy.logicalType = boolType(joinNullable({l, r}));
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
    copy.logicalType = boolType(joinNullable(newChildren));
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

/// Mangled-name overload entries: keys are built by
/// `UnboundLogicalFunction::withInferredLogicalType` from the children's
/// LogicalType names. With the prototype's reduced logical-type vocabulary,
/// integer constants surface as "INTEGER", so the scalar*Point and Point*scalar
/// overloads use the `INTEGER` key (not the legacy `INT64`).

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEquals_Point_PointLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return RegisterPointEqualsLogicalFunction(std::move(arguments));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterAdd_Point_PointLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return RegisterPointAddLogicalFunction(std::move(arguments));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterMul_Point_INTEGERLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return RegisterPointScaleLogicalFunction(std::move(arguments));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterMul_INTEGER_PointLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "Mul_INTEGER_Point requires exactly two children, got {}", arguments.children.size());
    std::swap(arguments.children[0], arguments.children[1]);
    return RegisterPointScaleLogicalFunction(std::move(arguments));
}

}
