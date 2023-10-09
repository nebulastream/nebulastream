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

#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/Expressions/ArithmeticalExpressions/CeilExpressionNode.hpp>
#include <Util/Logger/Logger.hpp>
namespace NES {

CeilExpressionNode::CeilExpressionNode(DataTypePtr stamp) : ArithmeticalUnaryExpressionNode(std::move(stamp)){};

CeilExpressionNode::CeilExpressionNode(CeilExpressionNode* other) : ArithmeticalUnaryExpressionNode(other) {}

ExpressionNodePtr CeilExpressionNode::create(ExpressionNodePtr const& child) {
    auto ceilNode = std::make_shared<CeilExpressionNode>(child->getStamp());
    ceilNode->setChild(child);
    return ceilNode;
}

void CeilExpressionNode::inferStamp(const Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext, SchemaPtr schema) {
    // infer stamp of child, check if its numerical, assume same stamp
    ArithmeticalUnaryExpressionNode::inferStamp(typeInferencePhaseContext, schema);

    // if stamp is integer, convert stamp to float
    stamp = DataTypeFactory::createFloatFromInteger(stamp);
    NES_TRACE("CeilExpressionNode: converted stamp to float: {}", toString());
}

bool CeilExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<CeilExpressionNode>()) {
        auto otherCeilNode = rhs->as<CeilExpressionNode>();
        return child()->equal(otherCeilNode->child());
    }
    return false;
}

std::string CeilExpressionNode::toString() const {
    std::stringstream ss;
    ss << "CEIL(" << children[0]->toString() << ")";
    return ss.str();
}

ExpressionNodePtr CeilExpressionNode::copy() { return CeilExpressionNode::create(children[0]->as<ExpressionNode>()->copy()); }

}// namespace NES