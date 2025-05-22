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
#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunctionWhen::NodeFunctionWhen(std::shared_ptr<DataType> stamp) : NodeFunctionBinary(std::move(stamp), "When") { };

NodeFunctionWhen::NodeFunctionWhen(NodeFunctionWhen* other) : NodeFunctionBinary(other)
{
}

std::shared_ptr<NodeFunction>
NodeFunctionWhen::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto whenNode = std::make_shared<NodeFunctionWhen>(left->getStamp());
    whenNode->setChildren(left, right);
    return whenNode;
}

void NodeFunctionWhen::inferStamp(const Schema& schema)
{
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    ///left function has to be boolean
    if (!NES::Util::instanceOf<Boolean>(left->getStamp()))
    {
        CannotInferStamp(
            "Error during stamp inference. Left type needs to be Boolean, but Left was: " + left->getStamp()->toString()
            + "Right was: " + right->getStamp()->toString());
    }

    ///set stamp to right stamp, as only the left function will be returned
    stamp = right->getStamp();
    NES_TRACE("NodeFunctionWhen: we assigned the following stamp: {}", stamp->toString())
}

bool NodeFunctionWhen::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionWhen>(rhs))
    {
        auto otherWhenNode = NES::Util::as<NodeFunctionWhen>(rhs);
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::ostream& NodeFunctionWhen::toDebugString(std::ostream& os) const
{
    PRECONDITION(children.size() == 2, "Cannot print function without exactly 2 children.");
    return os << fmt::format("WHEN({}, {})", *children.at(0), *children.at(1));
}

std::shared_ptr<NodeFunction> NodeFunctionWhen::deepCopy()
{
    return NodeFunctionWhen::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

bool NodeFunctionWhen::validateBeforeLowering() const
{
    /// left function has to be boolean
    const auto functionLeft = this->getLeft();
    return NES::Util::instanceOf<Boolean>(functionLeft->getStamp());
}

}
