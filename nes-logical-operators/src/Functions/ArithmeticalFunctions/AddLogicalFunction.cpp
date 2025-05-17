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
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
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

AddLogicalFunction::AddLogicalFunction(const LogicalFunction& left, const LogicalFunction& right)
    : dataType(left.getDataType()->join(*right.getDataType())), left(left), right(right)
{
}

AddLogicalFunction::AddLogicalFunction(const AddLogicalFunction& other) : dataType(other.dataType), left(other.left), right(other.right)
{
}

std::shared_ptr<DataType> AddLogicalFunction::getDataType() const
{
    return dataType;
};

LogicalFunction AddLogicalFunction::withDataType(std::shared_ptr<DataType> dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
};

LogicalFunction AddLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredDataType(schema));
    }
    return this->withChildren(newChildren);
}

std::vector<LogicalFunction> AddLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction AddLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "AddLogicalFunction requires exactly two children, but got {}", children.size());
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.dataType = children[0].getDataType()->join(*children[1].getDataType());
    return copy;
};

std::string_view AddLogicalFunction::getType() const
{
    return NAME;
}

bool AddLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    const auto* other = dynamic_cast<const AddLogicalFunction*>(&rhs);
    if (other != nullptr)
    {
        const bool simpleMatch = left == other->left and right == other->right;
        const bool commutativeMatch = right == other->right and right == other->left;
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string AddLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("AddLogicalFunction({} + {} : {})", left.explain(verbosity), right.explain(verbosity), dataType->toString());
    }
    return fmt::format("{} + {}", left.explain(verbosity), right.explain(verbosity));
}

SerializableFunction AddLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAddLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "AddLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return AddLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
