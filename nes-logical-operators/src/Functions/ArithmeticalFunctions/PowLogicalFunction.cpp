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

#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>

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

namespace NES
{

PowLogicalFunction::PowLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(left.getDataType().join(right.getDataType()).value_or(DataType{DataType::Type::UNDEFINED})), left(left), right(right) { };

bool PowLogicalFunction::operator==(const PowLogicalFunction& rhs) const
{
    const bool simpleMatch = left == rhs.left and right == rhs.right;
    const bool commutativeMatch = left == rhs.right and right == rhs.left;
    return simpleMatch or commutativeMatch;
}

std::string PowLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("POW({}, {})", left.explain(verbosity), right.explain(verbosity));
}

DataType PowLogicalFunction::getDataType() const
{
    return dataType;
};

PowLogicalFunction PowLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction PowLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newLeft = left.withInferredDataType(schema);
    const auto newRight = right.withInferredDataType(schema);
    return withDataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64)).withChildren({newLeft, newRight});
};

std::vector<LogicalFunction> PowLogicalFunction::getChildren() const
{
    return {left, right};
};

PowLogicalFunction PowLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    return copy;
};

std::string_view PowLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<PowLogicalFunction>::operator()(const PowLogicalFunction& function) const
{
    return reflect(detail::ReflectedPowLogicalFunction{.left = function.left, .right = function.right});
}

PowLogicalFunction Unreflector<PowLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [left, right] = context.unreflect<detail::ReflectedPowLogicalFunction>(reflected);
    return PowLogicalFunction{left, right};
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterPowLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 2)
    {
        throw CannotDeserialize("Function requires exactly two children, but got {}", arguments.children.size());
    }
    return PowLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
