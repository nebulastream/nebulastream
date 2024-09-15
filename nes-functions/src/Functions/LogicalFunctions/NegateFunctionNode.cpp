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

#include <Functions/LogicalFunctions/NegateFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES
{

NegateFunctionNode::NegateFunctionNode() : LogicalUnaryFunctionNode("Negate")
{
};

NegateFunctionNode::NegateFunctionNode(NegateFunctionNode* other) : LogicalUnaryFunctionNode(other)
{
}

bool NegateFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NegateFunctionNode>())
    {
        auto other = rhs->as<NegateFunctionNode>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}

std::string NegateFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "!" << children[0]->toString();
    return ss.str();
}

FunctionNodePtr NegateFunctionNode::create(FunctionNodePtr const& child)
{
    auto equals = std::make_shared<NegateFunctionNode>();
    equals->setChild(child);
    return equals;
}

void NegateFunctionNode::inferStamp(SchemaPtr schema)
{
    /// delegate stamp inference of children
    FunctionNode::inferStamp(schema);
    /// check if children stamp is correct
    if (!child()->isPredicate())
    {
        NES_THROW_RUNTIME_ERROR("Negate Function Node: the stamp of child must be boolean, but was: " + child()->getStamp()->toString());
    }
}
FunctionNodePtr NegateFunctionNode::deepCopy()
{
    return NegateFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool NegateFunctionNode::validateBeforeLowering() const
{
    return children.size() == 1;
}

}