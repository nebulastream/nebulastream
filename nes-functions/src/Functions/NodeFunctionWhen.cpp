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
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Logger/Logger.hpp>
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

void NodeFunctionWhen::inferStamp(SchemaPtr schema)
{
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    ///left function has to be boolean
    if (!left->getStamp()->isBoolean())
    {
        NES_THROW_RUNTIME_ERROR(
            "Error during stamp inference. Left type needs to be Boolean, but Left was: {} Right was: {}",
            left->getStamp()->toString(),
            right->getStamp()->toString());
    }

    ///set stamp to right stamp, as only the left function will be returned
    stamp = right->getStamp();
    NES_TRACE("NodeFunctionWhen: we assigned the following stamp: {}", stamp->toString())
}

bool NodeFunctionWhen::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionWhen>())
    {
        auto otherWhenNode = rhs->as<NodeFunctionWhen>();
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::string NodeFunctionWhen::toString() const
{
    std::stringstream ss;
    ss << "WHEN(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

NodeFunctionPtr NodeFunctionWhen::deepCopy()
{
    return NodeFunctionWhen::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool NodeFunctionWhen::validateBeforeLowering() const
{
    /// left function has to be boolean
    const auto functionLeft = this->getLeft();
    return functionLeft->getStamp()->isBoolean();
}

}
