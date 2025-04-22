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
#include <DataTypes/DataType.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionArithmeticalBinary.hpp>
#include <Functions/ArithmeticalFunctions/NodeFunctionDiv.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <ErrorHandling.hpp>
namespace NES
{

NodeFunctionDiv::NodeFunctionDiv(DataType stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Div") {};

NodeFunctionDiv::NodeFunctionDiv(NodeFunctionDiv* other) : NodeFunctionArithmeticalBinary(other)
{
}

std::shared_ptr<NodeFunction> NodeFunctionDiv::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto divNode = std::make_shared<NodeFunctionDiv>(left->getStamp());
    divNode->setChildren(left, right);
    return divNode;
}

bool NodeFunctionDiv::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionDiv>(rhs))
    {
        auto otherDivNode = NES::Util::as<NodeFunctionDiv>(rhs);
        return getLeft()->equal(otherDivNode->getLeft()) && getRight()->equal(otherDivNode->getRight());
    }
    return false;
}

std::ostream& NodeFunctionDiv::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " / " << *children.at(1);
}

std::shared_ptr<NodeFunction> NodeFunctionDiv::deepCopy()
{
    return NodeFunctionDiv::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}


}
