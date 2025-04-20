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
#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/MulBinaryLogicalFunction.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
MulBinaryLogicalFunction::MulBinaryLogicalFunction(std::shared_ptr<DataType> stamp) : BinaryLogicalFunction(std::move(stamp), "Mul") {};

MulBinaryLogicalFunction::MulBinaryLogicalFunction(MulBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
MulBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto mulNode = std::make_shared<MulBinaryLogicalFunction>(left->getStamp());
    mulNode->setChildren(left, right);
    return mulNode;
}

bool MulBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<MulBinaryLogicalFunction>(rhs))
    {
        const auto otherMulNode = NES::Util::as<MulBinaryLogicalFunction>(rhs);
        const bool simpleMatch = getLeft()->equal(otherMulNode->getLeft()) and getRight()->equal(otherMulNode->getRight());
        const bool commutativeMatch = getLeft()->equal(otherMulNode->getRight()) and getRight()->equal(otherMulNode->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::string MulBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "*" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> MulBinaryLogicalFunction::clone() const
{
    return MulBinaryLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
