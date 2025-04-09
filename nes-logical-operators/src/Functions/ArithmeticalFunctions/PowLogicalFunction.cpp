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
#include <Functions/ArithmeticalFunctions/PowLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

PowLogicalFunction::PowLogicalFunction(LogicalFunction left, LogicalFunction right) :stamp(left.getStamp()->join(right.getStamp())), left(left), right(right)
{
};

PowLogicalFunction::PowLogicalFunction(const PowLogicalFunction& other) : stamp(other.stamp), left(other.left), right(other.right)
{
}

bool PowLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (auto other = dynamic_cast<const PowLogicalFunction*>(&rhs))
    {
        return left == other->left && right == other->right;
    }
    return false;
}

std::string PowLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "POWER(" << left << ", " << right << ")";
    return ss.str();
}

std::shared_ptr<DataType> PowLogicalFunction::getStamp() const
{
    return stamp;
};

LogicalFunction PowLogicalFunction::withStamp(std::shared_ptr<DataType> stamp) const
{
    auto copy = *this;
    copy.stamp = stamp;
    return copy;
};

LogicalFunction PowLogicalFunction::withInferredStamp(Schema schema) const
{
    std::vector<LogicalFunction> newChildren;
    for (auto& child : getChildren())
    {
        newChildren.push_back(child.withInferredStamp(schema));
    }
    return withChildren(newChildren);
};

std::vector<LogicalFunction> PowLogicalFunction::getChildren() const
{
    return {left, right};
};

LogicalFunction PowLogicalFunction::withChildren(std::vector<LogicalFunction> children) const
{
    auto copy = *this;
    copy.left = children[0];
    copy.right = children[1];
    copy.stamp = children[0].getStamp()->join(children[1].getStamp());
    return copy;
};

std::string PowLogicalFunction::getType() const
{
    return std::string(NAME);
}

SerializableFunction PowLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);
    serializedFunction.add_children()->CopyFrom(left.serialize());
    serializedFunction.add_children()->CopyFrom(right.serialize());
    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());
    return serializedFunction;
}

BinaryLogicalFunctionRegistryReturnType
BinaryLogicalFunctionGeneratedRegistrar::RegisterPowBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return PowLogicalFunction(arguments.leftChild, arguments.rightChild);
}

}
