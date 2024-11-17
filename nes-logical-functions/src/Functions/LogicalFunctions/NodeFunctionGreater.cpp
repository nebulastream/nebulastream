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

#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

NodeFunctionGreater::NodeFunctionGreater() noexcept : NodeFunctionLogicalBinary("Greater")
{
}

NodeFunctionGreater::NodeFunctionGreater(NodeFunctionGreater* other) : NodeFunctionLogicalBinary(other)
{
}

NodeFunctionPtr NodeFunctionGreater::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
{
    auto greater = std::make_shared<NodeFunctionGreater>();
    greater->setChildren(left, right);
    return greater;
}

bool NodeFunctionGreater::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionGreater>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionGreater>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string NodeFunctionGreater::toString() const
{
    std::stringstream ss;
    ss << *children[0] << ">" << *children[1];
    return ss.str();
}

NodeFunctionPtr NodeFunctionGreater::deepCopy()
{
    return NodeFunctionGreater::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
