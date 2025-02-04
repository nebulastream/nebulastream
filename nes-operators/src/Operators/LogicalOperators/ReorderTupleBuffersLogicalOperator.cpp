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

#include <Operators/LogicalOperators/ReorderBuffersLogicalOperator.hpp>

namespace NES {

ReorderTupleBuffersLogicalOperator::ReorderTupleBuffersLogicalOperator(OperatorId id): Operator(id), LogicalUnaryOperator(id) { }

std::string ReorderTupleBuffersLogicalOperator::toString() const {
    std::stringstream ss;
    ss << "REORDERTUPLEBUFFERS(" << id << ")";
    return ss.str();
}

bool ReorderTupleBuffersLogicalOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<ReorderTupleBuffersLogicalOperator>()->getId() == id;
}

bool ReorderTupleBuffersLogicalOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<ReorderTupleBuffersLogicalOperator>()) {
        return true;
    }
    return false;
}

OperatorPtr ReorderTupleBuffersLogicalOperator::copy() {
    auto copy = LogicalOperatorFactory::createReorderTuplesOperator(id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setZ3Signature(z3Signature);
    copy->setOperatorState(operatorState);
    copy->setStatisticId(statisticId);
    for (const auto& pair : properties) {
        copy->addProperty(pair.first, pair.second);
    }
    return copy;
}

bool ReorderTupleBuffersLogicalOperator::inferSchema() {
    if (!LogicalUnaryOperator::inferSchema()) {
        return false;
    }
    return true;
}

void ReorderTupleBuffersLogicalOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }

    std::stringstream signatureStream;
    auto childSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << "REORDER()." << *childSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}