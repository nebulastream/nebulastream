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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/ArithmeticalFunctions/AbsoluteLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child)
{
}

AbsoluteLogicalFunction::AbsoluteLogicalFunction(const AbsoluteLogicalFunction& other) : dataType(other.dataType), child(other.child)
{
}

std::shared_ptr<DataType> AbsoluteLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction AbsoluteLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction AbsoluteLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> AbsoluteLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction AbsoluteLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
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

bool AbsoluteLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
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
        return fmt::format("AbsoluteLogicalFunction({} : {})", child.explain(verbosity), dataType->toString());
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

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterAbsoluteLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(
        arguments.children.size() == 1, "AbsoluteLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return AbsoluteLogicalFunction(arguments.children[0]);
}

}
