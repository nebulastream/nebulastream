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

#include <utility>
#include <API/Schema.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
NodeFunctionWhen::NodeFunctionWhen(DataTypePtr stamp) : NodeFunctionBinary(std::move(stamp), "When") {};

NodeFunctionWhen::NodeFunctionWhen(NodeFunctionWhen* other) : NodeFunctionBinary(other)
{
}

NodeFunctionPtr NodeFunctionWhen::create(const NodeFunctionPtr& left, const NodeFunctionPtr& right)
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

bool NodeFunctionWhen::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionWhen>(rhs))
    {
        auto otherWhenNode = NES::Util::as<NodeFunctionWhen>(rhs);
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::string NodeFunctionWhen::toString() const
{
    std::stringstream ss;
    ss << "WHEN(" << *children[0] << "," << *children[1] << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionWhen::deepCopy()
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
