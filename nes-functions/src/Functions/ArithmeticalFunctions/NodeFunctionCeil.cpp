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

#include <Functions/ArithmeticalFunctions/NodeFunctionCeil.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

NodeFunctionCeil::NodeFunctionCeil(DataType stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Ceil") {};

NodeFunctionCeil::NodeFunctionCeil(NodeFunctionCeil* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionCeil::create(NodeFunctionPtr const& child)
{
    auto ceilNode = std::make_shared<NodeFunctionCeil>(child->getStamp());
    ceilNode->setChild(child);
    return ceilNode;
}

void NodeFunctionCeil::inferStamp(Schema& schema)
{
    /// infer stamp of the child, check if its numerical, assume the same stamp
    NodeFunctionArithmeticalUnary::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("NodeFunctionCeil: converted stamp to float: {}", toString());
}

bool NodeFunctionCeil::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCeil>(rhs))
    {
        auto otherCeilNode = NES::Util::as<NodeFunctionCeil>(rhs);
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

std::string NodeFunctionCeil::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << children[0]->toString() << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionCeil::deepCopy()
{
    return NodeFunctionCeil::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
