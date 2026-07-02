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

#include <Functions/OctetLengthLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <utility>
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

OctetLengthLogicalFunction::OctetLengthLogicalFunction(LogicalFunction child) : child(std::move(child))
{
}

bool OctetLengthLogicalFunction::operator==(const OctetLengthLogicalFunction& rhs) const
{
    return child == rhs.child;
}

std::string OctetLengthLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("OCTET_LENGTH({})", child.explain(verbosity));
}

LogicalFunction OctetLengthLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.child = child.withInferredDataType(schema);
    if (not copy.child.getDataType().isType(DataType::Type::VARSIZED))
    {
        throw DifferentFieldTypeExpected("OCTET_LENGTH expects a VARSIZED input but got {}", copy.child.getDataType());
    }
    copy.dataType = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    copy.dataType.nullable = copy.child.getDataType().nullable;
    return copy;
}

DataType OctetLengthLogicalFunction::getDataType() const
{
    return dataType;
};

std::vector<LogicalFunction> OctetLengthLogicalFunction::getChildren() const
{
    return {child};
};

OctetLengthLogicalFunction OctetLengthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "OctetLengthLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view OctetLengthLogicalFunction::getType() const
{
    return NAME;
}

Reflected Reflector<OctetLengthLogicalFunction>::operator()(const OctetLengthLogicalFunction& function) const
{
    return reflect(detail::ReflectedOctetLengthLogicalFunction{.child = function.child});
}

OctetLengthLogicalFunction
Unreflector<OctetLengthLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [child] = context.unreflect<detail::ReflectedOctetLengthLogicalFunction>(reflected);
    return OctetLengthLogicalFunction(std::move(child));
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterOCTET_LENGTHLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("OCTET_LENGTH requires exactly one argument, but got {}", arguments.children.size());
    }
    return OctetLengthLogicalFunction(arguments.children.back());
}

}
