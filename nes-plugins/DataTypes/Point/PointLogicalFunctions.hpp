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

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/LogicalType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// Constructor function: `Point(x, y, z)` builds a Point from three FLOAT64 components.
class PointConstructLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "Point";

    explicit PointConstructLogicalFunction(std::vector<LogicalFunction> children);

    [[nodiscard]] bool operator==(const PointConstructLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointConstructLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointConstructLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    std::vector<LogicalFunction> children;

    friend Reflector<PointConstructLogicalFunction>;
};

class PointAddLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "PointAdd";

    PointAddLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const PointAddLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointAddLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointAddLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    LogicalFunction left, right;

    friend Reflector<PointAddLogicalFunction>;
};

class PointSubLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "PointSub";

    PointSubLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const PointSubLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointSubLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointSubLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    LogicalFunction left, right;

    friend Reflector<PointSubLogicalFunction>;
};

class PointScaleLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "PointScale";

    PointScaleLogicalFunction(LogicalFunction point, LogicalFunction scalar);

    [[nodiscard]] bool operator==(const PointScaleLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointScaleLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointScaleLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    LogicalFunction point, scalar;

    friend Reflector<PointScaleLogicalFunction>;
};

class PointDistanceLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "PointDistance";

    PointDistanceLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const PointDistanceLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointDistanceLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointDistanceLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    LogicalFunction left, right;

    friend Reflector<PointDistanceLogicalFunction>;
};

/// Component-wise equality. Result is a scalar Bool. Registered under the
/// mangled overload key `Equals_Point_Point` so the resolver in
/// `UnboundLogicalFunction::withInferredDataType` picks it up when both
/// operands have logical type `Point`.
class PointEqualsLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "PointEquals";

    PointEqualsLogicalFunction(LogicalFunction left, LogicalFunction right);

    [[nodiscard]] bool operator==(const PointEqualsLogicalFunction& rhs) const;

    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] LogicalType getLogicalType() const;
    [[nodiscard]] PointEqualsLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] PointEqualsLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    LogicalType logicalType;
    LogicalFunction left, right;

    friend Reflector<PointEqualsLogicalFunction>;
};

template <>
struct Reflector<PointConstructLogicalFunction>
{
    Reflected operator()(const PointConstructLogicalFunction& function) const;
};
template <>
struct Unreflector<PointConstructLogicalFunction>
{
    PointConstructLogicalFunction operator()(const Reflected& reflected) const;
};
template <>
struct Reflector<PointAddLogicalFunction>
{
    Reflected operator()(const PointAddLogicalFunction& function) const;
};
template <>
struct Unreflector<PointAddLogicalFunction>
{
    PointAddLogicalFunction operator()(const Reflected& reflected) const;
};
template <>
struct Reflector<PointSubLogicalFunction>
{
    Reflected operator()(const PointSubLogicalFunction& function) const;
};
template <>
struct Unreflector<PointSubLogicalFunction>
{
    PointSubLogicalFunction operator()(const Reflected& reflected) const;
};
template <>
struct Reflector<PointScaleLogicalFunction>
{
    Reflected operator()(const PointScaleLogicalFunction& function) const;
};
template <>
struct Unreflector<PointScaleLogicalFunction>
{
    PointScaleLogicalFunction operator()(const Reflected& reflected) const;
};
template <>
struct Reflector<PointDistanceLogicalFunction>
{
    Reflected operator()(const PointDistanceLogicalFunction& function) const;
};
template <>
struct Unreflector<PointDistanceLogicalFunction>
{
    PointDistanceLogicalFunction operator()(const Reflected& reflected) const;
};
template <>
struct Reflector<PointEqualsLogicalFunction>
{
    Reflected operator()(const PointEqualsLogicalFunction& function) const;
};
template <>
struct Unreflector<PointEqualsLogicalFunction>
{
    PointEqualsLogicalFunction operator()(const Reflected& reflected) const;
};

static_assert(LogicalFunctionConcept<PointConstructLogicalFunction>);
static_assert(LogicalFunctionConcept<PointAddLogicalFunction>);
static_assert(LogicalFunctionConcept<PointSubLogicalFunction>);
static_assert(LogicalFunctionConcept<PointScaleLogicalFunction>);
static_assert(LogicalFunctionConcept<PointDistanceLogicalFunction>);
static_assert(LogicalFunctionConcept<PointEqualsLogicalFunction>);

}

namespace NES::detail
{
struct ReflectedPointFunction
{
    std::vector<std::optional<LogicalFunction>> children;
};
}

FMT_OSTREAM(NES::PointConstructLogicalFunction);
FMT_OSTREAM(NES::PointAddLogicalFunction);
FMT_OSTREAM(NES::PointSubLogicalFunction);
FMT_OSTREAM(NES::PointScaleLogicalFunction);
FMT_OSTREAM(NES::PointDistanceLogicalFunction);
FMT_OSTREAM(NES::PointEqualsLogicalFunction);
