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

#include <Functions/LogicalFunctions/NodeFunctionNegate.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

NodeFunctionNegate::NodeFunctionNegate() : NodeFunctionLogicalUnary("Negate")
{
};

NodeFunctionNegate::NodeFunctionNegate(NodeFunctionNegate* other) : NodeFunctionLogicalUnary(other)
{
}

bool NodeFunctionNegate::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionNegate>())
    {
        auto other = rhs->as<NodeFunctionNegate>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}

std::string NodeFunctionNegate::toString() const
{
    std::stringstream ss;
    ss << "!" << children[0]->toString();
    return ss.str();
}

NodeFunctionPtr NodeFunctionNegate::create(NodeFunctionPtr const& child)
{
    auto equals = std::make_shared<NodeFunctionNegate>();
    equals->setChild(child);
    return equals;
}

void NodeFunctionNegate::inferStamp(SchemaPtr schema)
{
    /// delegate stamp inference of children
    FunctionNode::inferStamp(schema);
    /// check if children stamp is correct
    if (!child()->isPredicate())
    {
        NES_THROW_RUNTIME_ERROR("Negate Function Node: the stamp of child must be boolean, but was: " + child()->getStamp()->toString());
    }
}
NodeFunctionPtr NodeFunctionNegate::deepCopy()
{
    return NodeFunctionNegate::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool NodeFunctionNegate::validateBeforeLowering() const
{
    return children.size() == 1;
}

}