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

#include <Operators/Exceptions/InvalidOperatorStateException.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/OperatorState.hpp>
#include <Util/QuerySignatureContext.hpp>
#include <utility>

namespace NES {

LogicalOperator::LogicalOperator(OperatorId id) : Operator(id) {}

Optimizer::QuerySignaturePtr LogicalOperator::getZ3Signature() const { return z3Signature; }

void LogicalOperator::inferZ3Signature(const Optimizer::QuerySignatureContext& context) {
    if (z3Signature) {
        return;
    }
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("Inferring Z3 expressions for {}", operatorNode->toString());

    //Infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferZ3Signature(context);
    }
    z3Signature = context.createQuerySignatureForOperator(operatorNode);
}

void LogicalOperator::setZ3Signature(Optimizer::QuerySignaturePtr signature) { this->z3Signature = std::move(signature); }

std::map<size_t, std::set<std::string>> LogicalOperator::getHashBasedSignature() const { return hashBasedSignature; }

void LogicalOperator::setHashBasedSignature(std::map<size_t, std::set<std::string>> signature) {
    this->hashBasedSignature = std::move(signature);
}

void LogicalOperator::updateHashBasedSignature(size_t hashCode, const std::string& stringSignature) {
    if (hashBasedSignature.contains(hashCode)) {
        auto stringSignatures = hashBasedSignature[hashCode];
        stringSignatures.emplace(stringSignature);
        hashBasedSignature[hashCode] = stringSignatures;
    } else {
        hashBasedSignature[hashCode] = {stringSignature};
    }
}

void LogicalOperator::setOperatorState(NES::OperatorState newOperatorState) {
    using enum OperatorState;
    //Set the new operator state after validating the previous state
    switch (newOperatorState) {
        case TO_BE_PLACED:
            // an operator in the state TO_BE_PLACED or TO_BE_REPLACED can be changed to TO_BE_PLACED
            if (this->operatorState == TO_BE_PLACED || this->operatorState == TO_BE_REPLACED) {
                this->operatorState = newOperatorState;
                break;
            }
            throw Exceptions::InvalidOperatorStateException(id, {TO_BE_REMOVED, PLACED}, this->operatorState);
        case TO_BE_REMOVED:
            if (this->operatorState != REMOVED) {
                this->operatorState = TO_BE_REMOVED;
                break;
            }
            // an operator can be marked as TO_BE_REMOVED only if it is not in the state REMOVED.
            throw Exceptions::InvalidOperatorStateException(
                id,
                                                            {TO_BE_REMOVED, TO_BE_PLACED, PLACED, TO_BE_REPLACED},
                                                            this->operatorState);
        case TO_BE_REPLACED:
            if (this->operatorState != REMOVED && this->operatorState != TO_BE_REMOVED) {
                this->operatorState = TO_BE_REPLACED;
                break;
            }
            // an operator can be marked as TO_BE_REPLACED only if it is not in the state REMOVED or TO_BE_REMOVED.
            throw Exceptions::InvalidOperatorStateException(id,
                                                            {TO_BE_PLACED, PLACED, OperatorState::TO_BE_REPLACED},
                                                            this->operatorState);
        case PLACED:
            if (this->operatorState != REMOVED && this->operatorState != TO_BE_REMOVED) {
                this->operatorState = PLACED;
                break;
            }
            // an operator can be marked as PLACED only if it is not in the state REMOVED or TO_BE_REMOVED or already PLACED.
            throw Exceptions::InvalidOperatorStateException(id, {TO_BE_PLACED, TO_BE_REPLACED}, this->operatorState);
        case REMOVED:
            if (this->operatorState == TO_BE_PLACED || this->operatorState == TO_BE_REMOVED) {
                this->operatorState = REMOVED;
                break;
            }
            // an operator can be marked as REMOVED only if it is in the state TO_BE_REMOVED.
            throw Exceptions::InvalidOperatorStateException(id, {TO_BE_REMOVED}, this->operatorState);
    }
}

OperatorState LogicalOperator::getOperatorState() const { return operatorState; }

}// namespace NES
