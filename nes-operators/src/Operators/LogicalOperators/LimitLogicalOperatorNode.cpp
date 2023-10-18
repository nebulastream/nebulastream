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

#include <Operators/LogicalOperators/LimitLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES {

LimitLogicalOperatorNode::LimitLogicalOperatorNode(uint64_t limit, uint64_t id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id), limit(limit) {}

uint64_t LimitLogicalOperatorNode::getLimit() const { return limit; }

bool LimitLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LimitLogicalOperatorNode>()->getId() == id;
}

bool LimitLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LimitLogicalOperatorNode>()) {
        auto limitOperator = rhs->as<LimitLogicalOperatorNode>();
        return limit == limitOperator->limit;
    }
    return false;
};

std::string LimitLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "LIMIT" << id << ")";
    return ss.str();
}

bool LimitLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {
    if (!LogicalUnaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }
    return true;
}

OperatorNodePtr LimitLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createLimitOperator(limit, id);
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

void LimitLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("LimitLogicalOperatorNode: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty(), "LimitLogicalOperatorNode: Limit should have children");

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << "LIMIT(" << limit << ")." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
