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
#include <Functions/LogicalFunctions/NodeFunctionLessEquals.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

NodeFunctionLessEquals::NodeFunctionLessEquals() : NodeFunctionLogicalBinary("LessEquals")
{
}

NodeFunctionLessEquals::NodeFunctionLessEquals(NodeFunctionLessEquals* other) : NodeFunctionLogicalBinary(other)
{
}

std::shared_ptr<NodeFunction>
NodeFunctionLessEquals::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto lessThen = std::make_shared<NodeFunctionLessEquals>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool NodeFunctionLessEquals::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionLessEquals>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionLessEquals>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::ostream& NodeFunctionLessEquals::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << *children.at(0) << " <= " << *children.at(1);
}

std::shared_ptr<NodeFunction> NodeFunctionLessEquals::deepCopy()
{
    return NodeFunctionLessEquals::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

}
