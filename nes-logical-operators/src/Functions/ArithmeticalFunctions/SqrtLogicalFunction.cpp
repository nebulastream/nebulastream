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
#include <Functions/ArithmeticalFunctions/SqrtLogicalFunction.hpp>
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

SqrtLogicalFunction::SqrtLogicalFunction(const LogicalFunction& child) : dataType(child.getDataType()), child(child) { };

SqrtLogicalFunction::SqrtLogicalFunction(const SqrtLogicalFunction& other) : dataType(other.dataType), child(other.child)
{
}

bool SqrtLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    const auto* other = dynamic_cast<const SqrtLogicalFunction*>(&rhs);
    if (other != nullptr)
    {
        return child == other->child;
    }
    return false;
}

std::string SqrtLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SqrtLogicalFunction({} : {})", child.explain(verbosity), dataType->toString());
    }
    return fmt::format("SQRT({})", child.explain(verbosity));
}

std::shared_ptr<DataType> SqrtLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction SqrtLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction SqrtLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> SqrtLogicalFunction::getChildren() const
{
    return {child};
};

LogicalFunction SqrtLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "SqrtLogicalFunction requires exactly one child, but got {}", children.size());
    auto copy = *this;
    copy.child = children[0];
    return copy;
};

std::string_view SqrtLogicalFunction::getType() const
{
    return NAME;
}

SerializableFunction SqrtLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(child.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSqrtLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1, "SqrtLogicalFunction requires exactly one child, but got {}", arguments.children.size());
    return SqrtLogicalFunction(arguments.children[0]);
}

}
