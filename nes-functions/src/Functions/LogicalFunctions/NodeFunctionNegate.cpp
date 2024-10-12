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
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{

NodeFunctionNegate::NodeFunctionNegate() : NodeFunctionLogicalUnary("Negate") {};

NodeFunctionNegate::NodeFunctionNegate(NodeFunctionNegate* other) : NodeFunctionLogicalUnary(other)
{
}

bool NodeFunctionNegate::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionNegate>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionNegate>(rhs);
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
    NodeFunction::inferStamp(schema);
    /// check if children stamp is correct
    if (!child()->isPredicate())
    {
        NES_THROW_RUNTIME_ERROR("Negate Function Node: the stamp of child must be boolean, but was: " + child()->getStamp()->toString());
    }
}
NodeFunctionPtr NodeFunctionNegate::deepCopy()
{
    return NodeFunctionNegate::create(Util::as<NodeFunction>(children[0])->deepCopy());
}

bool NodeFunctionNegate::validateBeforeLowering() const
{
    return children.size() == 1;
}

}
