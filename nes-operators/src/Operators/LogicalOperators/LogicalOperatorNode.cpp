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

#include <Exceptions/InvalidOperatorStateException.hpp>
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/OperatorState.hpp>
#include <utility>

namespace NES {

LogicalOperatorNode::LogicalOperatorNode(uint64_t id)
    : OperatorNode(id), z3Signature(nullptr), hashBasedSignature(), hashGenerator(), operatorState(OperatorState::TO_BE_PLACED) {}

Optimizer::QuerySignaturePtr LogicalOperatorNode::getZ3Signature() { return z3Signature; }

void LogicalOperatorNode::inferZ3Signature(const z3::ContextPtr& context) {
    if (z3Signature) {
        return;
    }
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("Inferring Z3 expressions for {}", operatorNode->toString());

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferZ3Signature(context);
    }
    z3Signature = Optimizer::Utils::createQuerySignatureForOperator(context, operatorNode);
}

void LogicalOperatorNode::setZ3Signature(Optimizer::QuerySignaturePtr signature) { this->z3Signature = std::move(signature); }

std::map<size_t, std::set<std::string>> LogicalOperatorNode::getHashBasedSignature() { return hashBasedSignature; }

void LogicalOperatorNode::setHashBasedSignature(std::map<size_t, std::set<std::string>> signature) {
    this->hashBasedSignature = std::move(signature);
}

void LogicalOperatorNode::updateHashBasedSignature(size_t hashCode, const std::string& stringSignature) {
    if (hashBasedSignature.find(hashCode) != hashBasedSignature.end()) {
        auto stringSignatures = hashBasedSignature[hashCode];
        stringSignatures.emplace(stringSignature);
        hashBasedSignature[hashCode] = stringSignatures;
    } else {
        hashBasedSignature[hashCode] = {stringSignature};
    }
}

void LogicalOperatorNode::setOperatorState(NES::OperatorState newOperatorState) {

    //Set the new operator state after validating the previous state
    switch (newOperatorState) {
        case OperatorState::TO_BE_PLACED:
            //TO_BE_PLACED is the default operator state when it gets created. Therefore, a new set operator status to TO_BE_PLACED
            // is not expected.
            throw Exceptions::InvalidOperatorStateException(
                id,
                {OperatorState::TO_BE_PLACED, OperatorState::TO_BE_REMOVED, OperatorState::TO_BE_REPLACED, OperatorState::PLACED},
                this->operatorState);
        case OperatorState::TO_BE_REMOVED:
            if (this->operatorState != OperatorState::REMOVED) {
                this->operatorState = OperatorState::TO_BE_REMOVED;
                break;
            }
            // an operator can be marked as TO_BE_REMOVED only if it is not in the state REMOVED.
            throw Exceptions::InvalidOperatorStateException(
                id,
                {OperatorState::TO_BE_REMOVED, OperatorState::TO_BE_PLACED, OperatorState::PLACED, OperatorState::TO_BE_REPLACED},
                this->operatorState);
        case OperatorState::TO_BE_REPLACED:
            if (this->operatorState != OperatorState::REMOVED && this->operatorState != OperatorState::TO_BE_REMOVED) {
                this->operatorState = OperatorState::TO_BE_REPLACED;
                break;
            }
            // an operator can be marked as TO_BE_REPLACED only if it is not in the state REMOVED or TO_BE_REMOVED.
            throw Exceptions::InvalidOperatorStateException(
                id,
                {OperatorState::TO_BE_PLACED, OperatorState::PLACED, OperatorState::TO_BE_REPLACED},
                this->operatorState);
        case OperatorState::PLACED:
            if (this->operatorState != OperatorState::REMOVED && this->operatorState != OperatorState::TO_BE_REMOVED
                && this->operatorState != OperatorState::PLACED) {
                this->operatorState = OperatorState::PLACED;
                break;
            }
            // an operator can be marked as PLACED only if it is not in the state REMOVED or TO_BE_REMOVED or already PLACED.
            throw Exceptions::InvalidOperatorStateException(id,
                                                            {OperatorState::TO_BE_PLACED, OperatorState::TO_BE_REPLACED},
                                                            this->operatorState);
        case OperatorState::REMOVED:
            if (this->operatorState == OperatorState::TO_BE_REMOVED) {
                this->operatorState = OperatorState::REMOVED;
                break;
            }
            // an operator can be marked as REMOVED only if it is in the state TO_BE_REMOVED.
            throw Exceptions::InvalidOperatorStateException(id, {OperatorState::TO_BE_REMOVED}, this->operatorState);
    }
}

OperatorState LogicalOperatorNode::getOperatorState() { return operatorState; }

}// namespace NES
