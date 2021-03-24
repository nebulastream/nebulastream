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
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>

namespace NES {

WatermarkAssignerLogicalOperatorNode::WatermarkAssignerLogicalOperatorNode(
    const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), watermarkStrategyDescriptor(watermarkStrategyDescriptor) {}

Windowing::WatermarkStrategyDescriptorPtr WatermarkAssignerLogicalOperatorNode::getWatermarkStrategyDescriptor() const {
    return watermarkStrategyDescriptor;
}

const std::string WatermarkAssignerLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << id << ")";
    return ss.str();
}

std::string WatermarkAssignerLogicalOperatorNode::getStringBasedSignature() {
    std::stringstream ss;
    ss << "WATERMARKASSIGNER(" << watermarkStrategyDescriptor->toString() << ").";
    ss << children[0]->as<LogicalOperatorNode>()->getStringBasedSignature();
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

bool WatermarkAssignerLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    watermarkStrategyDescriptor->inferStamp(inputSchema);
    return true;
}

}// namespace NES