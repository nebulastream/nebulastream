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
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalBinaryOperator.hpp>
#include <Operators/LogicalOperators/LogicalUnionOperator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

LogicalUnionOperator::LogicalUnionOperator(OperatorId id) : Operator(id), LogicalBinaryOperator(id) {}

bool LogicalUnionOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalUnionOperator>()->getId() == id;
}

std::string LogicalUnionOperator::toString() const {
    std::stringstream ss;
    ss << "unionWith(" << id << ")";
    return ss.str();
}

bool LogicalUnionOperator::inferSchema() {
    if (!LogicalBinaryOperator::inferSchema()) {
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
        NES_ERROR("Found Schema mismatch for left and right schema types. Left schema {} and Right schema {} ",
                  leftInputSchema->toString(),
                  rightInputSchema->toString());
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

OperatorPtr LogicalUnionOperator::copy() {
    auto copy = LogicalOperatorFactory::createUnionOperator(id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOutputSchema(outputSchema);
    copy->setOperatorState(operatorState);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalUnionOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalUnionOperator>()) {
        auto rhsUnion = rhs->as<LogicalUnionOperator>();
        return leftInputSchema->equals(rhsUnion->getLeftInputSchema()) && outputSchema->equals(rhsUnion->getOutputSchema());
    }
    return false;
}

void LogicalUnionOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalUnionOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "LogicalUnionOperator: Union should have 2 children.");
    //Infer query signatures for child operators
    for (auto&& child : children) {
        child->as<LogicalOperator>()->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "UNION(";
    auto rightChildSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

void LogicalUnionOperator::inferInputOrigins() {

    // in the default case we collect all input origins from the children/upstream operators
    std::vector<uint64_t> combinedInputOriginIds;
    for (auto child : this->children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferInputOrigins();
        auto childInputOriginIds = childOperator->getOutputOriginIds();
        combinedInputOriginIds.insert(combinedInputOriginIds.end(), childInputOriginIds.begin(), childInputOriginIds.end());
    }
    this->leftInputOriginIds = combinedInputOriginIds;
}

}// namespace NES