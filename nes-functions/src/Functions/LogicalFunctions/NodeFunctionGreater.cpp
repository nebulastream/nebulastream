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
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunctions/NodeFunctionGreater.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionGreater::NodeFunctionGreater() noexcept : NodeFunctionLogicalBinary("Greater")
{
}

NodeFunctionGreater::NodeFunctionGreater(NodeFunctionGreater* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction>
NodeFunctionGreater::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto greater = std::make_shared<NodeFunctionGreater>();
    greater->setChildren(left, right);
    return greater;
}

bool NodeFunctionGreater::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionGreater>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionGreater>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::ostream& NodeFunctionGreater::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children[0] << " > " << *children[1];
}

std::shared_ptr<NodeFunction> NodeFunctionGreater::deepCopy()
{
    return NodeFunctionGreater::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
