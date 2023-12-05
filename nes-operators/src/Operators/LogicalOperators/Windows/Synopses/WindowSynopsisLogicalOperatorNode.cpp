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

#include "Operators/LogicalOperators/Windows/Synopses/WindowSynopsisLogicalOperatorNode.hpp"
#include "API/Schema.hpp"
#include "Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp"
#include "Operators/LogicalOperators/Windows/Synopses/CountMinSynopsisDescriptor.hpp"
#include "Operators/LogicalOperators/Windows/Synopses/DDSketchSynopsisDescriptor.hpp"
#include "Operators/LogicalOperators/Windows/Synopses/HLLSynopsisDescriptor.hpp"
#include "Operators/LogicalOperators/Windows/Synopses/ReservoirSampleSynopsisDescriptor.hpp"
#include "Operators/LogicalOperators/Windows/Synopses/WindowSynopsisDescriptor.hpp"

namespace NES::Experimental::Statistics {

WindowSynopsisLogicalOperatorNode::WindowSynopsisLogicalOperatorNode(WindowSynopsisDescriptorPtr synopsisDescriptor,
                                                                     OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), synopsisDescriptor(synopsisDescriptor) {}

std::string WindowSynopsisLogicalOperatorNode::toString() const { return ""; }

OperatorNodePtr WindowSynopsisLogicalOperatorNode::copy() {
    return LogicalOperatorFactory::createSynopsisOperator(synopsisDescriptor);
}

bool WindowSynopsisLogicalOperatorNode::equal(const NES::NodePtr& rhs) const {
    if (rhs->instanceOf<WindowSynopsisLogicalOperatorNode>()) {
        auto synopsisOperatorNode = rhs->as<WindowSynopsisLogicalOperatorNode>();
        return this->synopsisDescriptor == synopsisOperatorNode->getSynopsisDescriptor();
    }
    return false;
}

bool WindowSynopsisLogicalOperatorNode::isIdentical(const NES::NodePtr& rhs) const {
    return this->equal(rhs) && this->getId() == rhs->as<WindowSynopsisLogicalOperatorNode>()->getId();
}

bool WindowSynopsisLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }

    auto inputSchema = getInputSchema();
    outputSchema->addField("DATA", BasicType::TEXT);
    synopsisDescriptor->addSynopsisSpecificFields(outputSchema);

    auto srcDesc = this->getParents()[0]->as<SourceLogicalOperatorNode>()->getSourceDescriptor();

    auto logicalSourceName = srcDesc->getLogicalSourceName();
    auto physicalSourceName = srcDesc->getPhysicalSourceName();
    return true;
}

void WindowSynopsisLogicalOperatorNode::inferStringSignature() {return;}

WindowSynopsisDescriptorPtr WindowSynopsisLogicalOperatorNode::getSynopsisDescriptor() const { return synopsisDescriptor; }
}// namespace NES::Experimental::Statistics