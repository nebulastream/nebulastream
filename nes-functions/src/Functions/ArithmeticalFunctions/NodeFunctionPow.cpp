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

#include <utility>
#include <Functions/ArithmeticalFunctions/NodeFunctionPow.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

NodeFunctionPow::NodeFunctionPow(DataType stamp) : NodeFunctionArithmeticalBinary(std::move(stamp), "Pow") {};

NodeFunctionPow::NodeFunctionPow(NodeFunctionPow* other) : NodeFunctionArithmeticalBinary(other)
{
}

NodeFunctionPtr NodeFunctionPow::create(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    auto powNode = std::make_shared<NodeFunctionPow>(float64());
    powNode->setChildren(left, right);
    return powNode;
}

void NodeFunctionPow::inferStamp(Schema& schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    NodeFunctionArithmeticalBinary::inferStamp(schema);
}

bool NodeFunctionPow::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionPow>(rhs))
    {
        auto otherAddNode = NES::Util::as<NodeFunctionPow>(rhs);
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string NodeFunctionPow::toString() const
{
    std::stringstream ss;
    ss << "POWER(" << children[0]->toString() << ", " << children[1]->toString() << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionPow::deepCopy()
{
    return NodeFunctionPow::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}
}
