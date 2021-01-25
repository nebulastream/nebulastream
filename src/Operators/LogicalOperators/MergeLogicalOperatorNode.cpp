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

#include <API/Schema.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/MergeLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>

namespace NES {

MergeLogicalOperatorNode::MergeLogicalOperatorNode(OperatorId id) : BinaryOperatorNode(id) {}

bool MergeLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<MergeLogicalOperatorNode>()->getId() == id;
}

const std::string MergeLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Merge(" << id << ")";
    return ss.str();
}

std::string MergeLogicalOperatorNode::getStringBasedSignature() {
    std::stringstream ss;
    ss << "MERGE(";
    ss << children[0]->as<LogicalOperatorNode>()->getStringBasedSignature() + ").";
    ss << children[1]->as<LogicalOperatorNode>()->getStringBasedSignature();
    return ss.str();
}

bool MergeLogicalOperatorNode::inferSchema() {
    if (!BinaryOperatorNode::inferSchema()) {
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

    //Copy the schema of left input
    outputSchema->clear();
    outputSchema->copyFields(leftInputSchema);
    return true;
}

OperatorNodePtr MergeLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createMergeOperator(id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    return copy;
}

bool MergeLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<MergeLogicalOperatorNode>()) {
        return true;
    }
    return false;
}

}// namespace NES