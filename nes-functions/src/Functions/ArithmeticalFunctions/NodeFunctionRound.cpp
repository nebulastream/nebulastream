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
#include <Functions/ArithmeticalFunctions/NodeFunctionRound.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/Integer.hpp>


namespace NES
{

NodeFunctionRound::NodeFunctionRound(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Round") {};

NodeFunctionRound::NodeFunctionRound(NodeFunctionRound* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionRound::create(NodeFunctionPtr const& child)
{
    auto roundNode = std::make_shared<NodeFunctionRound>(child->getStamp());
    roundNode->setChild(child);
    return roundNode;
}

bool NodeFunctionRound::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionRound>(rhs))
    {
        auto otherRoundNode = NES::Util::as<NodeFunctionRound>(rhs);
        return child()->equal(otherRoundNode->child());
    }
    return false;
}

std::string NodeFunctionRound::toString() const
{
    std::stringstream ss;
    ss << "ROUND(" << *children[0] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionRound::deepCopy()
{
    return NodeFunctionRound::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
