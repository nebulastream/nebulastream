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
#include <API/AttributeField.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/Statistics/WindowStatisticLogicalOperatorNode.hpp>
#include <StatisticFieldIdentifiers.hpp>
#include <sstream>

namespace NES::Experimental::Statistics {

WindowStatisticLogicalOperatorNode::WindowStatisticLogicalOperatorNode(WindowStatisticDescriptorPtr statisticDescriptor,
                                                                       OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), statisticDescriptor(statisticDescriptor) {}

std::string WindowStatisticLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "SatisticLogicalOperator: ";
    statisticDescriptor->toString();
    return ss.str();
}

OperatorNodePtr WindowStatisticLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createStatisticOperator(statisticDescriptor, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool WindowStatisticLogicalOperatorNode::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<WindowStatisticLogicalOperatorNode>()) {
        auto statisticOperatorNode = rhs->as<WindowStatisticLogicalOperatorNode>();
        if (statisticDescriptor == statisticOperatorNode->getStatisticDescriptor()) {
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

    auto logSourceNameWithSep = statisticDescriptor->getLogicalSourceName() + "$";

    outputSchema->clear();
    outputSchema->addField(logSourceNameWithSep + LOGICAL_SOURCE_NAME, BasicType::TEXT);
    outputSchema->addField(logSourceNameWithSep + PHYSICAL_SOURCE_NAME, BasicType::TEXT);
    outputSchema->addField(logSourceNameWithSep + FIELD_NAME, BasicType::TEXT);
    outputSchema->addField(logSourceNameWithSep + OBSERVED_TUPLES, BasicType::UINT64);
    outputSchema->addField(logSourceNameWithSep + DEPTH, BasicType::UINT64);
    outputSchema->addField(logSourceNameWithSep + START_TIME, BasicType::UINT64);
    outputSchema->addField(logSourceNameWithSep + END_TIME, BasicType::UINT64);

    outputSchema->addField(logSourceNameWithSep + DATA, BasicType::TEXT);

    statisticDescriptor->addStatisticFields(outputSchema);

    return true;
}

void WindowStatisticLogicalOperatorNode::inferStringSignature() {}

const WindowStatisticDescriptorPtr& WindowStatisticLogicalOperatorNode::getStatisticDescriptor() const {
    return statisticDescriptor;
}
}// namespace NES::Experimental::Statistics
