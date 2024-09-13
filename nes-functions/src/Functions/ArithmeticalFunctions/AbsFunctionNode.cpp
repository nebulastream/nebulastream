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

#include <cmath>
#include <Functions/ArithmeticalFunctions/AbsFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/StdInt.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

AbsFunctionNode::AbsFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp), "Abs") {};

AbsFunctionNode::AbsFunctionNode(AbsFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr AbsFunctionNode::create(const FunctionNodePtr& child)
{
    auto absNode = std::make_shared<AbsFunctionNode>(child->getStamp());
    absNode->setChild(child);
    return absNode;
}

void AbsFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// increase lower bound to 0
    stamp = DataTypeFactory::copyTypeAndIncreaseLowerBound(stamp, 0_s64);
    NES_TRACE("AbsFunctionNode: increased the lower bound of stamp to 0: {}", toString());
}

bool AbsFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<AbsFunctionNode>())
    {
        auto otherAbsNode = rhs->as<AbsFunctionNode>();
        return child()->equal(otherAbsNode->child());
    }
    return false;
}

std::string AbsFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "ABS(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr AbsFunctionNode::deepCopy()
{
    return AbsFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool AbsFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}
