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

#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>

#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

DataType AbsoluteLogicalFunction::getDataType() const
{
    return dataType;
};

AbsoluteLogicalFunction AbsoluteLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction AbsoluteLogicalFunction::withInferredDataType(const Schema& schema) const
{
    const auto newChild = child.withInferredDataType(schema);
    return withDataType(newChild.getDataType()).withChildren({newChild});
};

std::vector<LogicalFunction> AbsoluteLogicalFunction::getChildren() const
{
    return {child};
};

AbsoluteLogicalFunction AbsoluteLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "AbsoluteLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view AbsoluteLogicalFunction::getType() const
{
    return NAME;
}

bool AbsoluteLogicalFunction::operator==(const AbsoluteLogicalFunction& rhs) const
{
    if (const auto* other = dynamic_cast<const AbsoluteLogicalFunction*>(&rhs))
    {
        return child == other->child;
    }
    return false;
}

std::string AbsoluteLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("AbsoluteLogicalFunction({} : {})", child.explain(verbosity), dataType);
    }
    return fmt::format("ABS({})", child.explain(verbosity));
}

SerializableFunction AbsoluteLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAbsLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    if (arguments.children.size() != 1)
    {
        throw CannotDeserialize("AbsoluteLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    }
    return AbsoluteLogicalFunction(arguments.children[0]);
}

}
