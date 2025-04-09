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

#include <sstream>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <LogicalFunctionRegistry.hpp>

namespace NES
{

DivLogicalFunction::DivLogicalFunction(LogicalFunction left, LogicalFunction right) : stamp(left.getStamp().clone()), left(left), right(right)
{
};

DivLogicalFunction::DivLogicalFunction(const DivLogicalFunction& other) : stamp(other.stamp->clone()), left(other.left), right(other.right)
{
}

bool DivLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    auto other = dynamic_cast<const DivLogicalFunction*>(&rhs);
    if (other)
    {
        return left == other->left and right == other->right;
    }
    return false;
}

const DataType& DivLogicalFunction::getStamp() const
{
    return *stamp;
};

LogicalFunction DivLogicalFunction::withStamp(std::unique_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp->clone();
    return copy;
};

LogicalFunction DivLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> DivLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction DivLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp().join(children[1].getStamp());
    return copy;
};

std::string DivLogicalFunction::getType() const
{
    return std::string(NAME);
}

std::string DivLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << left << "/" << right;
    return ss.str();
}

SerializableFunction DivLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterDivLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    return DivLogicalFunction(arguments.children[0], arguments.children[1]);
}


}
