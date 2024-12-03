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
#include <ostream>
#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionMul.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunctionMul::NodeFunctionMul(std::shared_ptr<DataType> stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Mul") {};

NodeFunctionMul::NodeFunctionMul(NodeFunctionMul* other) : NodeFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionMul::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto mulNode = std::make_shared<NodeFunctionMul>(left->getStamp());
    mulNode->setChildren(left, right);
    return mulNode;
}

bool NodeFunctionMul::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionMul>(rhs))
    {
        const auto otherMulNode = NES::Util::as<NodeFunctionMul>(rhs);
        const bool simpleMatch = getLeft()->equal(otherMulNode->getLeft()) and getRight()->equal(otherMulNode->getRight());
        const bool commutativeMatch = getLeft()->equal(otherMulNode->getRight()) and getRight()->equal(otherMulNode->getLeft());
        return simpleMatch or commutativeMatch;
    }
    return false;
}

std::ostream& NodeFunctionMul::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " * " << *children.at(1);
}

std::shared_ptr<NodeFunction> NodeFunctionMul::deepCopy()
{
    return NodeFunctionMul::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
