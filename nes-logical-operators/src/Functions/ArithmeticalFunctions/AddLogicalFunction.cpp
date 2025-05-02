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
#include <sstream>
#include <Functions/ArithmeticalFunctions/AddLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <fmt/format.h>

namespace NES
{

AddLogicalFunction::AddLogicalFunction(LogicalFunction left, LogicalFunction right)
    : stamp(left.getStamp()->join(*right.getStamp())), left(left), right(right)
{
}

AddLogicalFunction::AddLogicalFunction(const AddLogicalFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

std::shared_ptr<DataType> AddLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction AddLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction AddLogicalFunction::withInferredStamp(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
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
    copy.stamp = children[0].getStamp()->join(*children[1].getStamp());
    return copy;
};

std::string_view AddLogicalFunction::getType() const
{
    return NAME;
}

bool AddLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const AddLogicalFunction*>(&rhs);
    if (other)
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
        return fmt::format("AddLogicalFunction({} + {} : {})", 
            left.explain(verbosity), 
            right.explain(verbosity),
            stamp->toString());
    }
    return fmt::format("{} + {}", left.explain(verbosity), right.explain(verbosity));
}

SerializableFunction AddLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAddLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2, "AddLogicalFunction requires exactly two children, but got {}", arguments.children.size());
    return AddLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
