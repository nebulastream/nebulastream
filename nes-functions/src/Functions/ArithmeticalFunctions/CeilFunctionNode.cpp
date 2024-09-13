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

#include <Functions/ArithmeticalFunctions/CeilFunctionNode.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

CeilFunctionNode::CeilFunctionNode(DataTypePtr stamp) : ArithmeticalUnaryFunctionNode(std::move(stamp), "Ceil") {};

CeilFunctionNode::CeilFunctionNode(CeilFunctionNode* other) : ArithmeticalUnaryFunctionNode(other)
{
}

FunctionNodePtr CeilFunctionNode::create(FunctionNodePtr const& child)
{
    auto ceilNode = std::make_shared<CeilFunctionNode>(child->getStamp());
    ceilNode->setChild(child);
    return ceilNode;
}

void CeilFunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp of the child, check if its numerical, assume the same stamp
    ArithmeticalUnaryFunctionNode::inferStamp(schema);

    /// if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("CeilFunctionNode: converted stamp to float: {}", toString());
}

bool CeilFunctionNode::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<CeilFunctionNode>(rhs))
    {
        auto otherCeilNode = NES::Util::as<CeilFunctionNode>(rhs);
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

std::string CeilFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "CEIL(" << children[0]->toString() << ")";
    return ss.str();
}

FunctionNodePtr CeilFunctionNode::deepCopy()
{
    return CeilFunctionNode::create(children[0]->as<FunctionNode>()->deepCopy());
}

bool CeilFunctionNode::validate() const
{
    NES_NOT_IMPLEMENTED();
}

}
