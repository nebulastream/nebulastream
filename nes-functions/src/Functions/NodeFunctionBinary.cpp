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
#include <Functions/NodeFunctionBinary.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
NodeFunctionBinary::NodeFunctionBinary(DataTypePtr stamp, std::string name) : NodeFunction(std::move(stamp), std::move(name))
{
}

NodeFunctionBinary::NodeFunctionBinary(NodeFunctionBinary* other) : NodeFunction(other)
{
    addChildWithEqual(getLeft()->deepCopy());
    addChildWithEqual(getRight()->deepCopy());
}

void NodeFunctionBinary::setChildren(NodeFunctionPtr const& left, NodeFunctionPtr const& right)
{
    addChildWithEqual(left);
    addChildWithEqual(right);
}

NodeFunctionPtr NodeFunctionBinary::getLeft() const
{
    INVARIANT(children.size() == 2, "A binary function always should have two children, but it had: " + std::to_string(children.size()));
    return Util::as<NodeFunction>(children[0]);
}

NodeFunctionPtr NodeFunctionBinary::getRight() const
{
    INVARIANT(children.size() == 2, "A binary function always should have two children, but it had: " + std::to_string(children.size()));
    return Util::as<NodeFunction>(children[1]);
}

}
