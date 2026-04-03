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

#include <Functions/FunctionPluginMacros.hpp>
#include "Functions/ConstantValueLogicalFunction.hpp"

/// Converts two UINT64 values (high bits, low bits) into a UUID string (VARSIZED).
namespace NES
{
NES_LOGICAL_FUNCTION(Uuid, UINT64, UINT64, VARSIZED);

NES_LOGICAL_FUNCTION_IMPL(To_String, 1)
{
    switch (children.at(0).getDataType().type)
    {
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::VARSIZED:
            return DataType::Type::VARSIZED;
        case DataType::Type::UNDEFINED:
            INVARIANT(children.at(0).getDataType().type != DataType::Type::UNDEFINED, "Data type should have been resolved");
    }
    std::unreachable();
}

static std::string getFormatString(const LogicalFunction& function)
{
    auto constantFunction = function.tryGetAs<ConstantValueLogicalFunction>();
    if (!constantFunction)
    {
        throw CannotInferSchema("FMT requires a constant string as its first argument");
    }
    if (constantFunction->getDataType() != DataTypeProvider::provideDataType("VARSIZED"))
    {
        throw CannotInferSchema("FMT requires a constant string as its first argument");
    }
    return constantFunction->get().getConstantValue();
}

class FMTLogicalFunction final
{
public:
    static constexpr std::string_view NAME = "FMT";
    explicit FMTLogicalFunction(std::vector<LogicalFunction> children);
    [[nodiscard]] bool operator==(const FMTLogicalFunction& rhs) const;
    [[nodiscard]] DataType getDataType() const;
    [[nodiscard]] FMTLogicalFunction withDataType(const DataType& dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;
    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] FMTLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;
    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    DataType dataType;
    std::vector<LogicalFunction> children_;
    friend Reflector<FMTLogicalFunction>;
};

static_assert(LogicalFunctionConcept<FMTLogicalFunction>);

template <>
struct Reflector<FMTLogicalFunction>
{
    Reflected operator()(const FMTLogicalFunction& function) const;
};

template <>
struct Unreflector<FMTLogicalFunction>
{
    FMTLogicalFunction operator()(const Reflected& reflected) const;
};
}

namespace NES::detail
{
struct ReflectedFMTLogicalFunction
{
    std::vector<LogicalFunction> children;
};
}

FMT_OSTREAM(NES::FMTLogicalFunction);

namespace NES
{
FMTLogicalFunction::FMTLogicalFunction(std::vector<LogicalFunction> children)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children_(std::move(children))
{
}

bool FMTLogicalFunction::operator==(const FMTLogicalFunction& rhs) const
{
    return children_ == rhs.children_;
}

DataType FMTLogicalFunction::getDataType() const
{
    return dataType;
}

FMTLogicalFunction FMTLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

std::string_view FMTLogicalFunction::getType() const
{
    return NAME;
}

std::vector<LogicalFunction> FMTLogicalFunction::getChildren() const
{
    return children_;
}

FMTLogicalFunction FMTLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children_ = children;
    return copy;
}

std::string FMTLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return detail::explainFunction(NAME, children_, verbosity);
}

LogicalFunction FMTLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto inferredChildren = children_
        | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); })
        | std::ranges::to<std::vector>();
    auto newDataType = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    newDataType.nullable = std::ranges::any_of(inferredChildren, [](const auto& c) { return c.getDataType().nullable; });

    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(inferredChildren.size() + 1);
    newChildren.emplace_back(inferredChildren[0]);

    auto formatString = getFormatString(newChildren[0]);
    size_t inArgument = false;
    size_t argCount = 1;
    for (auto c : formatString)
    {
        if (!inArgument && c == '{')
        {
            inArgument = true;
        }
        else if (inArgument && c == '}')
        {
            inArgument = false;
            auto inputFunction = inferredChildren.at(argCount++);
            if (inputFunction.getDataType().type != DataType::Type::VARSIZED)
            {
                newChildren.emplace_back(To_StringLogicalFunction({inputFunction}).withInferredDataType(schema));
            }
            else
            {
                newChildren.emplace_back(inputFunction);
            }
        }
    }

    return withDataType(newDataType).withChildren(newChildren);
}

Reflected Reflector<FMTLogicalFunction>::operator()(const FMTLogicalFunction& function) const
{
    return reflect(detail::ReflectedFMTLogicalFunction{.children = function.children_});
}

FMTLogicalFunction Unreflector<FMTLogicalFunction>::operator()(const Reflected& reflected) const
{
    auto result = unreflect<detail::ReflectedFMTLogicalFunction>(reflected);
    return FMTLogicalFunction{std::move(result.children)};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterFMTLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<FMTLogicalFunction>(arguments.reflected);
    }
    if (arguments.children.size() < 1)
    {
        throw CannotDeserialize(
            "FMT"
            "LogicalFunction requires at least {} children, but got {}",
            1,
            arguments.children.size());
    }
    return FMTLogicalFunction(std::move(arguments.children));
}
}
