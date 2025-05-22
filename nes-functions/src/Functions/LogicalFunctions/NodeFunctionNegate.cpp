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
#include <API/Schema.hpp>
#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Functions/NodeFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>

namespace NES
{

NodeFunctionNegate::NodeFunctionNegate() : NodeFunctionLogicalUnary("Negate") { };

NodeFunctionNegate::NodeFunctionNegate(NodeFunctionNegate* other) : NodeFunctionLogicalUnary(other)
{
}

bool NodeFunctionNegate::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionNegate>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionNegate>(rhs);
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}

std::ostream& NodeFunctionNegate::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 1, "Cannot print function without exactly one child.");
    return os << "!" << *children.front();
}

std::shared_ptr<NodeFunction> NodeFunctionNegate::create(const std::shared_ptr<NodeFunction>& child)
{
    auto equals = std::make_shared<NodeFunctionNegate>();
    equals->setChild(child);
    return equals;
}

void NodeFunctionNegate::inferStamp(const Schema& schema)
{
    /// delegate stamp inference of children
    NodeFunction::inferStamp(schema);
    /// check if children stamp is correct
    if (!child()->isPredicate())
    {
        throw CannotInferSchema(
            fmt::format("Negate Function Node: the stamp of child must be boolean, but was: {}", child()->getStamp()->toString()));
    }
}

std::shared_ptr<NodeFunction> NodeFunctionNegate::deepCopy()
{
    return NodeFunctionNegate::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

bool NodeFunctionNegate::validateBeforeLowering() const
{
    if (children.size() != 1)
    {
        return false;
    }
    return NES::Util::instanceOf<Boolean>(Util::as<NodeFunction>(children[0])->getStamp());
}

}
