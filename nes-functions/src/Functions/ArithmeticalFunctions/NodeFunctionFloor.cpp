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
#include <Functions/ArithmeticalFunctions/NodeFunctionFloor.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

NodeFunctionFloor::NodeFunctionFloor(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Floor") {};

NodeFunctionFloor::NodeFunctionFloor(NodeFunctionFloor* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionFloor::create(NodeFunctionPtr const& child)
{
    auto floorNode = std::make_shared<NodeFunctionFloor>(child->getStamp());
    floorNode->setChild(child);
    return floorNode;
}

bool NodeFunctionFloor::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionFloor>(rhs))
    {
        auto otherFloorNode = NES::Util::as<NodeFunctionFloor>(rhs);
        return child()->equal(otherFloorNode->child());
    }
    return false;
}

std::string NodeFunctionFloor::toString() const
{
    std::stringstream ss;
    ss << "FLOOR(" << *children[0] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionFloor::deepCopy()
{
    return NodeFunctionFloor::create(Util::as<NodeFunction>(children[0])->deepCopy());
}
}
