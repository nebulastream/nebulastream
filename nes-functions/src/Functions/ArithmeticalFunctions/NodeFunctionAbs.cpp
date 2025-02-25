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
#include <Functions/ArithmeticalFunctions/NodeFunctionAbs.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

NodeFunctionAbs::NodeFunctionAbs(DataTypePtr stamp) : NodeFunctionArithmeticalUnary(std::move(stamp), "Abs") {};

NodeFunctionAbs::NodeFunctionAbs(NodeFunctionAbs* other) : NodeFunctionArithmeticalUnary(other)
{
}

NodeFunctionPtr NodeFunctionAbs::create(const NodeFunctionPtr& child)
{
    auto absNode = std::make_shared<NodeFunctionAbs>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

bool NodeFunctionAbs::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionAbs>(rhs))
    {
        auto otherAbsNode = NES::Util::as<NodeFunctionAbs>(rhs);
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

std::string NodeFunctionAbs::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << *children[0] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionAbs::deepCopy()
{
    return NodeFunctionAbs::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

}
