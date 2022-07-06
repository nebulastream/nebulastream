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
#include <Nodes/Expressions/ArithmeticalExpressions/ArithmeticalBinaryExpressionNode.hpp>
#include <utility>
namespace NES {

ArithmeticalBinaryExpressionNode::ArithmeticalBinaryExpressionNode(DataTypePtr stamp)
    : BinaryExpressionNode(std::move(stamp)), ArithmeticalExpressionNode() {}
ArithmeticalBinaryExpressionNode::ArithmeticalBinaryExpressionNode(ArithmeticalBinaryExpressionNode* other)
    : BinaryExpressionNode(other) {}

/**
 * @brief The current implementation of type inference for arithmetical expressions expects that both
 * operands of an arithmetical expression have numerical stamps.
 * If this is valid we derived the joined stamp of the left and right operand.
 * (e.g., left:int8, right:int32 -> int32)
 * @param schema the current schema we use during type inference.
 */
void ArithmeticalBinaryExpressionNode::inferStamp(SchemaPtr schema) {
    // infer the stamps of the left and right child
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    // both sub expressions have to be numerical
    if (!left->getStamp()->isNumeric() || !right->getStamp()->isNumeric()) {
        throw std::logic_error(
            "ArithmeticalBinaryExpressionNode: Error during stamp inference. Types need to be Numerical but Left was:"
            + left->getStamp()->toString() + " Right was: " + right->getStamp()->toString());
    }

    // calculate the common stamp by joining the left and right stamp
    auto commonStamp = left->getStamp()->join(right->getStamp());

    // check if the common stamp is defined
    if (commonStamp->isUndefined()) {
        // the common stamp was not valid -> in this case the common stamp is undefined.
        throw std::logic_error("ArithmeticalBinaryExpressionNode: " + commonStamp->toString()
                               + " is not supported by arithmetical expressions");
    }

    stamp = commonStamp;
    NES_TRACE("ArithmeticalBinaryExpressionNode: we assigned the following stamp: " << toString());
}

bool ArithmeticalBinaryExpressionNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ArithmeticalBinaryExpressionNode>()) {
        auto otherAddNode = rhs->as<ArithmeticalBinaryExpressionNode>();
        return getLeft()->equal(otherAddNode->getLeft()) && getRight()->equal(otherAddNode->getRight());
    }
    return false;
}

std::string ArithmeticalBinaryExpressionNode::toString() const { return "ArithmeticalBinaryExpression()"; }

}// namespace NES