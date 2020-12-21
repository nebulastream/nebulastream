/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Windowing/Watermark/WatermarkStrategy.hpp>

namespace NES {

WatermarkAssignerLogicalOperatorNode::WatermarkAssignerLogicalOperatorNode(
    const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id)
    : watermarkStrategyDescriptor(watermarkStrategyDescriptor), UnaryOperatorNode(id) {}

const std::string WatermarkAssignerLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

bool WatermarkAssignerLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<WatermarkAssignerLogicalOperatorNode>()->getId() == id;
}
bool WatermarkAssignerLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<WatermarkAssignerLogicalOperatorNode>()) {
        auto watermarkAssignerOperator = rhs->as<WatermarkAssignerLogicalOperatorNode>();
        return watermarkStrategyDescriptor->equal(watermarkAssignerOperator->getWatermarkStrategyDescriptor());
    }
    return false;
}
OperatorNodePtr WatermarkAssignerLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createWatermarkAssignerOperator(watermarkStrategyDescriptor, id);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    return copy;
}

Windowing::WatermarkStrategyDescriptorPtr WatermarkAssignerLogicalOperatorNode::getWatermarkStrategyDescriptor() const {
    return watermarkStrategyDescriptor;
}

}// namespace NES