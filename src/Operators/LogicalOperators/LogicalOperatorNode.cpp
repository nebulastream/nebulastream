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

#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/Signature/QuerySignature.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <utility>

namespace NES {

LogicalOperatorNode::LogicalOperatorNode(uint64_t id)
    : OperatorNode(id), z3Signature(nullptr), hashBasedSignature(), hashGenerator() {}

Optimizer::QuerySignaturePtr LogicalOperatorNode::getZ3Signature() { return z3Signature; }

void LogicalOperatorNode::inferZ3Signature(const z3::ContextPtr& context) {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("Inferring Z3 expressions for " << operatorNode->toString());

    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferZ3Signature(context);
    }
    z3Signature = Optimizer::QuerySignatureUtil::createQuerySignatureForOperator(context, operatorNode);
}

void LogicalOperatorNode::setZ3Signature(Optimizer::QuerySignaturePtr signature) { this->z3Signature = std::move(signature); }

std::map<size_t, std::set<std::string>> LogicalOperatorNode::getHashBasedSignature() { return hashBasedSignature; }

void LogicalOperatorNode::setHashBasedSignature(std::map<size_t, std::set<std::string>> signature) {
    this->hashBasedSignature = std::move(signature);
}

void LogicalOperatorNode::updateHashBasedSignature(size_t hashCode, std::string stringSignature) {

    if (hashBasedSignature.find(hashCode) != hashBasedSignature.end()) {
        auto stringSignatures = hashBasedSignature[hashCode];
        stringSignatures.emplace(stringSignature);
        hashBasedSignature[hashCode] = stringSignatures;
    } else {
        hashBasedSignature[hashCode] = {stringSignature};
    }
}
}// namespace NES
