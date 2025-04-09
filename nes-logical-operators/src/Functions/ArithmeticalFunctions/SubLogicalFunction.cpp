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
#include <Functions/ArithmeticalFunctions/SubLogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/Common.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

SubLogicalFunction::SubLogicalFunction(LogicalFunction left, LogicalFunction right)
    : stamp(left.getStamp().clone()), left(left), right(right) {};

SubLogicalFunction::SubLogicalFunction(const SubLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

bool SubLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const SubLogicalFunction*>(&rhs))
    {
        return left == other->left and right == other->right;
    }
    return false;
}

std::string SubLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "-" << right;
    return ss.str();
}

const DataType& SubLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction SubLogicalFunction::withStamp(std::unique_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp->clone();
    return copy;
};

LogicalFunction SubLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return this->withChildren(newChildren);
};

std::vector<LogicalFunction> SubLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction SubLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp().join(children[1].getStamp());
    return copy;
};

std::string SubLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction SubLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterSubLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return SubLogicalFunction(arguments.children[0], arguments.children[1]);
}

}
