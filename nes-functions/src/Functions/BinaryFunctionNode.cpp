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
#include <Functions/BinaryFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
BinaryFunctionNode::BinaryFunctionNode(DataTypePtr stamp, std::string name) : FunctionNode(std::move(stamp), std::move(name))
{
}

BinaryFunctionNode::BinaryFunctionNode(BinaryFunctionNode* other) : FunctionNode(other)
{
    addChildWithEqual(getLeft()->deepCopy());
    addChildWithEqual(getRight()->deepCopy());
}

void BinaryFunctionNode::setChildren(FunctionNodePtr const& left, FunctionNodePtr const& right)
{
    addChildWithEqual(left);
    addChildWithEqual(right);
}

FunctionNodePtr BinaryFunctionNode::getLeft() const
{
    if (children.size() != 2)
    {
        NES_FATAL_ERROR("A binary function always should have two children, but it had: {}", children.size());
    }
    return Util::as<FunctionNode>(children[0]);
}

FunctionNodePtr BinaryFunctionNode::getRight() const
{
    if (children.size() != 2)
    {
        NES_FATAL_ERROR("A binary function always should have two children, but it had: {}", children.size());
    }
    return Util::as<FunctionNode>(children[1]);
}

} /// namespace NES
