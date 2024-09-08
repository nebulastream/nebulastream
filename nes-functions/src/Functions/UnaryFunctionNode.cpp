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
#include <Functions/UnaryFunctionNode.hpp>

namespace NES
{
UnaryFunctionNode::UnaryFunctionNode(DataTypePtr stamp) : FunctionNode(std::move(stamp))
{
}

UnaryFunctionNode::UnaryFunctionNode(UnaryFunctionNode* other) : FunctionNode(other)
{
}

void UnaryFunctionNode::setChild(const FunctionNodePtr& child)
{
    addChildWithEqual(child);
}
FunctionNodePtr UnaryFunctionNode::child() const
{
    return children[0]->as<FunctionNode>();
}
} /// namespace NES
