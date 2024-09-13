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
#include <Functions/WhenFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
WhenFunctionNode::WhenFunctionNode(DataTypePtr stamp) : BinaryFunctionNode(std::move(stamp), "When") {};

WhenFunctionNode::WhenFunctionNode(WhenFunctionNode* other) : BinaryFunctionNode(other)
{
}

FunctionNodePtr WhenFunctionNode::create(const FunctionNodePtr& left, const FunctionNodePtr& right)
{
    auto whenNode = std::make_shared<WhenFunctionNode>(left->getStamp());
    whenNode->setChildren(left, right);
    return whenNode;
}

void WhenFunctionNode::inferStamp(SchemaPtr schema)
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
    NES_TRACE("WhenFunctionNode: we assigned the following stamp: {}", stamp->toString())
}

bool WhenFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<WhenFunctionNode>(rhs))
    {
        auto otherWhenNode = NES::Util::as<WhenFunctionNode>(rhs);
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::string WhenFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "WHEN(" << children[0]->toString() << "," << children[1]->toString() << ")";
    return ss.str();
}

FunctionNodePtr WhenFunctionNode::deepCopy()
{
    return WhenFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy(), children[1]->as<FunctionNode>()->deepCopy());
}

bool WhenFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}
