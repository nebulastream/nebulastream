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
#include <utility>

#include <Functions/ArithmeticalFunctions/NodeFunctionAdd.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionAdd::NodeFunctionAdd(DataTypePtr stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Add") {};

NodeFunctionAdd::NodeFunctionAdd(NodeFunctionAdd* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionAdd::create(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    auto addNode = std::make_shared<NodeFunctionAdd>(left->getStamp());
    addNode->setChildren(left, right);
    return addNode;
}

bool NodeFunctionAdd::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAdd>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionAdd>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string NodeFunctionAdd::toString() const
{
    std::stringstream ss;
    ss << children[0]->toString() << "+" << children[1]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionAdd::deepCopy()
{
    return NodeFunctionAdd::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
