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

#include "Nodes/Expressions/LinearAlgebraExpressions/CreateTensorExpressionNode.hpp"
#include "Common/DataTypes/DataType.hpp"
#include "Common/DataTypes/DataTypeFactory.hpp"

namespace NES {

CreateTensorExpressionNode::CreateTensorExpressionNode(DataTypePtr stamp,
                                                       const std::vector<size_t> shape,
                                                       const TensorMemoryFormat tensorType)
    : LinearAlgebraTensorExpressionNode(std::move(stamp)), shape(shape), tensorMemoryFormat(tensorType)  {}

CreateTensorExpressionNode::CreateTensorExpressionNode(CreateTensorExpressionNode* other)
    : LinearAlgebraTensorExpressionNode(other) {}

ExpressionNodePtr CreateTensorExpressionNode::create(std::vector<ExpressionNodePtr> const expressionNodes,
                                                     const std::vector<size_t> shape,
                                                     const TensorMemoryFormat tensorType) {
    auto createTensorNode = std::make_shared<CreateTensorExpressionNode>(
        DataTypeFactory::createTensor(shape, expressionNodes.at(0)->getStamp(), tensorType), shape, tensorType);
    createTensorNode->setChildren(expressionNodes);
    return createTensorNode;
}

bool CreateTensorExpressionNode::equal(const NodePtr& rhs) const {
    if (rhs->instanceOf<CreateTensorExpressionNode>()) {
        auto otherCreatTensorNode = rhs->as<CreateTensorExpressionNode>();
        bool equals = MultiExpressionNode::children().size() == otherCreatTensorNode->children().size();
        for (unsigned i = 0; i < MultiExpressionNode::children().size(); i++) {
            equals = MultiExpressionNode::children()[i]->equal(otherCreatTensorNode->children()[i]);
        }
        return equals;
    }
    return false;
}

std::string CreateTensorExpressionNode::toString() const {
    std::stringstream ss;
    ss << "[";
    for (auto child : Node::children) {
        ss << child->toString() << ", ";
    }
    ss << "]";
    return ss.str();
}

ExpressionNodePtr CreateTensorExpressionNode::copy() {
    return std::make_shared<CreateTensorExpressionNode>(CreateTensorExpressionNode(this));
}
const std::vector<size_t>& CreateTensorExpressionNode::getShape() const { return shape; }
void CreateTensorExpressionNode::setShape(const std::vector<size_t>& shape) { CreateTensorExpressionNode::shape = shape; }
TensorMemoryFormat CreateTensorExpressionNode::getTensorMemoryFormat() const { return tensorMemoryFormat; }
void CreateTensorExpressionNode::setTensorMemoryFormat(TensorMemoryFormat tensorMemoryFormat) {
    CreateTensorExpressionNode::tensorMemoryFormat = tensorMemoryFormat;
}

}// namespace NES