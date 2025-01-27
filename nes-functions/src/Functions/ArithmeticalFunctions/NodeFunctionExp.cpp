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

#include <cmath>
#include <Functions/ArithmeticalFunctions/NodeFunctionExp.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include "Functions/NodeFunction.hpp"
#include "Nodes/Node.hpp"


namespace NES
{

NodeFunctionExp::NodeFunctionExp(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Exp") {};

NodeFunctionExp::NodeFunctionExp(NodeFunctionExp* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionExp::create(const NodeFunctionPtr& child)
{
    auto expNode = std::make_shared<NodeFunctionExp>(child->getStamp());
    expNode->setChild(child);
    return expNode;
}

bool NodeFunctionExp::equal(const NodePtr& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionExp>(rhs))
    {
        auto otherExpNode = NES::Util::as<NodeFunctionExp>(rhs);
        return child()->equal(otherExpNode->child());
    }
    return false;
}

std::string NodeFunctionExp::toString() const
{
    std::stringstream ss;
    ss << "EXP(" << *children[0] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionExp::deepCopy()
{
    return NodeFunctionExp::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
