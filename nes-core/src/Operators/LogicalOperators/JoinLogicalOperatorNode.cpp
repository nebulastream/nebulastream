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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowTypes/WindowType.hpp>
#include <utility>
#include <z3++.h>

namespace NES {

JoinLogicalOperatorNode::JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id)
    : OperatorNode(id), LogicalBinaryOperatorNode(id), joinDefinition(std::move(joinDefinition)) {}

bool JoinLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<JoinLogicalOperatorNode>()->getId() == id;
}

std::string JoinLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Join(" << id << ")";
    return ss.str();
}

Join::LogicalJoinDefinitionPtr JoinLogicalOperatorNode::getJoinDefinition() { return joinDefinition; }

bool JoinLogicalOperatorNode::inferSchema() {

    if (!LogicalBinaryOperatorNode::inferSchema()) {
        return false;
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() != 2) {
        throw TypeInferenceException("BinaryOperatorNode: Found " + std::to_string(distinctSchemas.size())
                                     + " distinct schemas but expected 2 distinct schemas.");
    }

    //reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    //Find the schema for left join key
    FieldAccessExpressionNodePtr leftJoinKey = joinDefinition->getLeftJoinKey();
    auto leftJoinKeyName = leftJoinKey->getFieldName();
    for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
        if ((*itr)->hasFieldName(leftJoinKeyName)) {
            leftInputSchema->copyFields(*itr);
            leftJoinKey->inferStamp(leftInputSchema);
            //remove the schema from distinct schema list
            distinctSchemas.erase(itr);
            break;
        }
        itr++;
    }

    //Find the schema for right join key
    FieldAccessExpressionNodePtr rightJoinKey = joinDefinition->getRightJoinKey();
    auto rightJoinKeyName = rightJoinKey->getFieldName();
    for (auto& schema : distinctSchemas) {
        if (schema->hasFieldName(rightJoinKeyName)) {
            rightInputSchema->copyFields(schema);
            rightJoinKey->inferStamp(rightInputSchema);
        }
    }

    //Check if left input schema was identified
    if (!leftInputSchema) {
        NES_ERROR("JoinLogicalOperatorNode: Left input schema is not initialized. Make sure that left join key is present : "
                  + leftJoinKeyName);
        throw TypeInferenceException("JoinLogicalOperatorNode: Left input schema is not initialized.");
    }

    //Check if right input schema was identified
    if (!rightInputSchema) {
        NES_ERROR("JoinLogicalOperatorNode: Right input schema is not initialized. Make sure that right join key is present : "
                  + rightJoinKeyName);
        throw TypeInferenceException("JoinLogicalOperatorNode: Right input schema is not initialized.");
    }

    //Check that both left and right schema should be different
    if (rightInputSchema->equals(leftInputSchema, false)) {
        NES_ERROR("JoinLogicalOperatorNode: Found both left and right input schema to be same.");
        throw TypeInferenceException("JoinLogicalOperatorNode: Found both left and right input schema to be same.");
    }

    NES_DEBUG("Binary infer left schema=" << leftInputSchema->toString() << " right schema=" << rightInputSchema->toString());
    NES_ASSERT(leftInputSchema->getSchemaSizeInBytes() != 0, "left schema is emtpy");
    NES_ASSERT(rightInputSchema->getSchemaSizeInBytes() != 0, "right schema is emtpy");
    //Infer stamp of window definition
    auto windowType = joinDefinition->getWindowType();
    windowType->inferStamp(leftInputSchema);

    //Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    auto streamNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    auto streamNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    auto newQualifierForSystemField = streamNameLeft + streamNameRight;
    outputSchema->addField(createField(newQualifierForSystemField + "$start", UINT64));
    outputSchema->addField(createField(newQualifierForSystemField + "$end", UINT64));
    outputSchema->addField(AttributeField::create(newQualifierForSystemField + "$key", leftJoinKey->getStamp()));

    // create dynamic fields to store all fields from left and right streams
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Outputschme for join=" << outputSchema->toString());
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateStreamTypes(leftInputSchema, rightInputSchema);
    return true;
}

OperatorNodePtr JoinLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createJoinOperator(joinDefinition, id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool JoinLogicalOperatorNode::equal(NodePtr const& rhs) const { return rhs->instanceOf<JoinLogicalOperatorNode>(); }

void JoinLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("JoinLogicalOperatorNode: Inferring String signature for " << operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "JoinLogicalOperatorNode: Join should have 2 children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorNodePtr childOperator = child->as<LogicalOperatorNode>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "JOIN(LEFT-KEY=" << joinDefinition->getLeftJoinKey()->toString() << ",";
    signatureStream << "RIGHT-KEY=" << joinDefinition->getRightJoinKey()->toString() << ",";
    signatureStream << "WINDOW-DEFINITION=" << joinDefinition->getWindowType()->toString() << ",";

    auto rightChildSignature = children[0]->as<LogicalOperatorNode>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperatorNode>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}

std::string JoinLogicalOperatorNode::getClassName(){
    return "JoinLogicalOperatorNode";
}

}// namespace NES