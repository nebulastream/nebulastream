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

#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticLogicalOperatorNode.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

WindowStatisticLogicalOperatorNode::WindowStatisticLogicalOperatorNode(WindowStatisticDescriptorPtr statisticDescriptor,
                                                                       const std::string& synopsisFieldName,
                                                                       uint64_t windowSize,
                                                                       uint64_t slideFactor,
                                                                       OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), statisticDescriptor(statisticDescriptor),
      synopsisFieldName(synopsisFieldName), windowSize(windowSize), slideFactor(slideFactor) {}

std::string WindowStatisticLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "StatisticLogicalOperator: ";
    ss << " synopsisFieldName: " << this->getSynopsisFieldName();
    ss << " windowSize: " << this->getWindowSize();
    ss << " slideFactor: " << this->getSlideFactor();
    return ss.str();
}

OperatorNodePtr WindowStatisticLogicalOperatorNode::copy() {
    return LogicalOperatorFactory::createStatisticOperator(statisticDescriptor, synopsisFieldName, windowSize, slideFactor);
}

bool WindowStatisticLogicalOperatorNode::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<WindowStatisticLogicalOperatorNode>()) {
        auto statisticOperatorNode = rhs->as<WindowStatisticLogicalOperatorNode>();
        if (statisticDescriptor == statisticOperatorNode->getStatisticDescriptor()
            && synopsisFieldName == statisticOperatorNode->getSynopsisFieldName()
            && windowSize == statisticOperatorNode->getWindowSize() && slideFactor == statisticOperatorNode->getSlideFactor()) {
            return true;
        }
    }
    return false;
}

bool WindowStatisticLogicalOperatorNode::isIdentical(const NES::NodePtr& rhs) const {
    return this->equal(rhs) && this->getId() == rhs->as<WindowStatisticLogicalOperatorNode>()->getId();
}

bool WindowStatisticLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }

    auto inputSchema = getInputSchema();
    outputSchema->addField("synopsisFieldName", BasicType::TEXT);
    statisticDescriptor->addStatisticFields(outputSchema);
    outputSchema->addField("synopsisData", BasicType::TEXT);

    return true;
}

void WindowStatisticLogicalOperatorNode::inferStringSignature() {}

const WindowStatisticDescriptorPtr& WindowStatisticLogicalOperatorNode::getStatisticDescriptor() const {
    return statisticDescriptor;
}

const std::string& WindowStatisticLogicalOperatorNode::getSynopsisFieldName() const { return synopsisFieldName; }

uint64_t WindowStatisticLogicalOperatorNode::getWindowSize() const { return windowSize; }

uint64_t WindowStatisticLogicalOperatorNode::getSlideFactor() const { return slideFactor; }
}// namespace NES::Experimental::Statistics
