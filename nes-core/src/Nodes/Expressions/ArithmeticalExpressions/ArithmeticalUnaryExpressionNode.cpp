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
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalUnaryExpressionNode.hpp>
#include <utility>
namespace NES {

ArithmeticalUnaryExpressionNode::ArithmeticalUnaryExpressionNode(DataTypePtr stamp) : UnaryExpressionNode(std::move(stamp)) {}
ArithmeticalUnaryExpressionNode::ArithmeticalUnaryExpressionNode(ArithmeticalUnaryExpressionNode* other)
    : UnaryExpressionNode(other) {}

/**
 * @brief The current implementation of type inference for arithmetical expressions expects that both
 * operands of an arithmetical expression have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void ArithmeticalUnaryExpressionNode::inferStamp(SchemaPtr schema) {
    // infer stamp of child
    auto child = this->child();
    child->inferStamp(schema);

    // get stamp from child
    auto child_stamp = child->getStamp();
    if (!child_stamp->isNumeric()) {
        throw std::logic_error(
            "ArithmeticalUnaryExpressionNode: Error during stamp inference. Type needs to be Numerical but Child was:"
            + child_stamp->toString());
    }

    this->stamp = child_stamp;
    NES_TRACE("ArithmeticalUnaryExpressionNode: we assigned the following stamp: " << toString());
}

bool ArithmeticalUnaryExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ArithmeticalUnaryExpressionNode>()) {
        auto otherAddNode = rhs->as<ArithmeticalUnaryExpressionNode>();
        return child()->equal(otherAddNode->child());
    }
    return false;
}

std::string ArithmeticalUnaryExpressionNode::toString() const { return "ArithmeticalExpression()"; }

}// namespace NES