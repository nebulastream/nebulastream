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
#include <Functions/LogicalFunctions/EqualsLogicalFunction.hpp>
#include <Abstract/LogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

EqualsLogicalFunction::EqualsLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(DataTypeFactory::createBoolean(), left, right)
{
}

EqualsLogicalFunction::EqualsLogicalFunction(const EqualsLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

bool EqualsLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<EqualsLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<EqualsLogicalFunction>(rhs);
        const bool simpleMatch = getLeftChild() == other->getLeftChild() and getRightChild() == other->getRightChild();
        const bool commutativeMatch = getLeftChild() == other->getRightChild() and getRightChild() == other->getLeftChild();
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string EqualsLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "==" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> EqualsLogicalFunction::clone() const
{
    return std::make_shared<EqualsLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool EqualsLogicalFunction::validateBeforeLowering() const
{
    const auto childLeft = Util::as<LogicalFunction>(getLeftChild());
    const auto childRight = Util::as<LogicalFunction>(getRightChild());

    /// If one of the children has a stamp of type text, the other child must also have a stamp of type text
    if (NES::Util::instanceOf<VariableSizedDataType>(childLeft->getStamp())
        != NES::Util::instanceOf<VariableSizedDataType>(childRight->getStamp()))
    {
        return false;
    }

    return true;
}

SerializableFunction EqualsLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    auto* funcDesc = new SerializableFunction_BinaryFunction();
    auto* leftChild = funcDesc->mutable_leftchild();
    leftChild->CopyFrom(getLeftChild()->serialize());
    auto* rightChild = funcDesc->mutable_rightchild();
    rightChild->CopyFrom(getRightChild()->serialize());

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

std::unique_ptr<BinaryLogicalFunctionRegistryReturnType>
BinaryLogicalFunctionGeneratedRegistrar::RegisterEqualsBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<EqualsLogicalFunction>(arguments.leftChild, arguments.rightChild);
}

}
