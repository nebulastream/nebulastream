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
#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperatorNode.hpp>
#include <Operators/LogicalOperators/UnionLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

UnionLogicalOperatorNode::UnionLogicalOperatorNode(OperatorId id) : OperatorNode(id), LogicalBinaryOperatorNode(id) {}

bool UnionLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<UnionLogicalOperatorNode>()->getId() == id;
}

std::string UnionLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "unionWith(" << id << ")";
    return ss.str();
}

bool UnionLogicalOperatorNode::inferSchema() {
    if (!LogicalBinaryOperatorNode::inferSchema()) {
        return false;
    }

    leftInputSchema->clear();
    rightInputSchema->clear();
    if (distinctSchemas.size() == 1) {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[0]);
    } else {
        leftInputSchema->copyFields(distinctSchemas[0]);
        rightInputSchema->copyFields(distinctSchemas[1]);
    }

    if (!leftInputSchema->hasEqualTypes(rightInputSchema)) {
        NES_ERROR("Found Schema mismatch for left and right schema types. Left schema " + leftInputSchema->toString()
                  + " and Right schema " + rightInputSchema->toString());
        throw TypeInferenceException("Found Schema mismatch for left and right schema types. Left schema "
                                     + leftInputSchema->toString() + " and Right schema " + rightInputSchema->toString());
    }

    if (leftInputSchema->getLayoutType() != rightInputSchema->getLayoutType()) {
        NES_ERROR("Left and right should have same memory layout");
        throw TypeInferenceException("Left and right should have same memory layout");
    }

    //Copy the schema of left input
    outputSchema->clear();
    outputSchema->copyFields(leftInputSchema);
    outputSchema->setLayoutType(leftInputSchema->getLayoutType());
    return true;
}

OperatorNodePtr UnionLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createUnionOperator(id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool UnionLogicalOperatorNode::equal(NodePtr const& rhs) const { return rhs->instanceOf<UnionLogicalOperatorNode>(); }
void UnionLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("UnionLogicalOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "UnionLogicalOperatorNode: Union should have 2 children.");
    //Infer query signatures for child operators
    for (auto&& child : children) {
        child->as<LogicalOperatorNode>()->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "UNION(";
    auto rightChildSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

}// namespace NES