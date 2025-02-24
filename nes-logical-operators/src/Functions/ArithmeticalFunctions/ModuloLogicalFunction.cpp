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
#include <Functions/ArithmeticalFunctions/ModuloLogicalFunction.hpp>
#include <Functions/BinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <BinaryLogicalFunctionRegistry.hpp>

namespace NES
{

ModuloLogicalFunction::ModuloLogicalFunction(const ModuloLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

ModuloLogicalFunction::ModuloLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right) : BinaryLogicalFunction(left->getStamp()->join(right->getStamp()), left, right)
{
}

bool ModuloLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<ModuloLogicalFunction>(rhs))
    {
        auto otherAddNode = NES::Util::as<ModuloLogicalFunction>(rhs);
        return getLeftChild() == otherAddNode->getLeftChild() and getRightChild() == otherAddNode->getRightChild();
    }
    return false;
}

std::string ModuloLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "%" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> ModuloLogicalFunction::clone() const
{
    return std::make_shared<ModuloLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

SerializableFunction ModuloLogicalFunction::serialize() const
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
BinaryLogicalFunctionGeneratedRegistrar::RegisterModuloBinaryLogicalFunction(BinaryLogicalFunctionRegistryArguments arguments)
{
    return std::make_unique<ModuloLogicalFunction>(arguments.leftChild, arguments.rightChild);
}


}
