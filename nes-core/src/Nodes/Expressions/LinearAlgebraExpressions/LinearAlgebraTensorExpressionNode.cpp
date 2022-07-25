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

#include "Nodes/Expressions/LinearAlgebraExpressions/LinearAlgebraTensorExpressionNode.hpp"
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES {

LinearAlgebraTensorExpressionNode::LinearAlgebraTensorExpressionNode(DataTypePtr stamp)
    : MultiExpressionNode(std::move(stamp)), LinearAlgebraExpressionNode() {}
LinearAlgebraTensorExpressionNode::LinearAlgebraTensorExpressionNode(LinearAlgebraTensorExpressionNode* other)
    : MultiExpressionNode(other) {}

void LinearAlgebraTensorExpressionNode::inferStamp(SchemaPtr schema) {
    bool firstSkipped = false;
    std::shared_ptr<ExpressionNode> previousChild;
    std::shared_ptr<DataType> commonStamp;
    for (auto child : MultiExpressionNode::children()) {
        child->inferStamp(schema);
        if (!child->getStamp()->isNumeric()) {
            throw std::logic_error(
                "LinearAlgebraTensorExpressionNode: Error during stamp inference. Type needs to be Numerical but was: "
                + child->getStamp()->toString());
        }
        if (firstSkipped != false) {
            commonStamp = previousChild->getStamp()->join(child->getStamp());
        }
        firstSkipped = true;
        previousChild = child;
    }

    // check if the common stamp is defined
    if (commonStamp->isUndefined()) {
        // the common stamp was not valid -> in this case the common stamp is undefined.
        throw std::logic_error("LinearAlgebraTensorExpressionNode: " + commonStamp->toString()
                               + " is not supported by arithmetical expressions");
    }
    if (stamp->isTensor()){
        stamp = DataTypeFactory::createTensor(stamp->as<TensorType>(stamp)->shape, commonStamp, stamp->as<TensorType>(stamp)->tensorMemoryFormat);
    } else {
        stamp = commonStamp;
    }
    NES_TRACE("LinearAlgebraTensorExpressionNode: we assigned the following stamp: " << toString());
}

bool LinearAlgebraTensorExpressionNode::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<LinearAlgebraTensorExpressionNode>()) {
        auto otherAddNode = rhs->as<LinearAlgebraTensorExpressionNode>();
        bool equals = MultiExpressionNode::children().size() == otherAddNode->children().size();
        for (unsigned i = 0; i < MultiExpressionNode::children().size(); i++) {
            equals = MultiExpressionNode::children()[i]->equal(otherAddNode->children()[i]);
        }
        return equals;
    }
    return false;
}

std::string LinearAlgebraTensorExpressionNode::toString() const { return "LinearAlgebraTensorExpression()"; }

}// namespace NES