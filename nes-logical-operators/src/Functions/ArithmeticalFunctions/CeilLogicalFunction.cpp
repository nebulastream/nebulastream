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

#include <Functions/ArithmeticalFunctions/CeilLogicalFunction.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

CeilLogicalFunction::CeilLogicalFunction(LogicalFunction child) : dataType(child.getDataType()), child(std::move(child)) { };

DataType CeilLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction CeilLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    CeilLogicalFunction copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (!copy.child.getDataType().isNumeric())
    {
        throw CannotInferStamp("Cannot apply ceil function on non-numeric input function {}", copy.child);
    }
    copy.dataType = copy.child.getDataType();
    return copy;
};

std::vector<LogicalFunction> CeilLogicalFunction::getChildren() const
{
    return {child};
};

CeilLogicalFunction CeilLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "CeilLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view CeilLogicalFunction::getType() const
{
    return NAME;
}

bool CeilLogicalFunction::operator==(const CeilLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string CeilLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("CeilLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("CEIL({})", child.explain(verbosity));
}

Reflected Reflector<CeilLogicalFunction>::operator()(const CeilLogicalFunction& function) const
{
    return reflect(detail::ReflectedCeilLogicalFunction{.child = function.child});
}

CeilLogicalFunction Unreflector<CeilLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedCeilLogicalFunction>(reflected);
    return CeilLogicalFunction(std::move(child));
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterCeilLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("Function requires exactly one child, but got {}", arguments.children.size());
    }
    return CeilLogicalFunction(arguments.children[0]);
}

}
