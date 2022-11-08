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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Exceptions/TypeInferenceException.hpp>
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <Optimizer/QuerySignatures/QuerySignatureUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>
#include <utility>
#include <z3++.h>

namespace NES {

JoinLogicalOperatorNode::JoinLogicalOperatorNode(Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id, OriginId originId)
    : OperatorNode(id), LogicalBinaryOperatorNode(id), OriginIdAssignmentOperator(id, originId),
      joinDefinition(std::move(joinDefinition)) {}

bool JoinLogicalOperatorNode::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<JoinLogicalOperatorNode>()->getId() == id;
}

std::string JoinLogicalOperatorNode::toString() const {
    std::stringstream ss;
    ss << "Join(" << id << ")";
    return ss.str();
}

Join::LogicalJoinDefinitionPtr JoinLogicalOperatorNode::getJoinDefinition() { return joinDefinition; }

bool JoinLogicalOperatorNode::inferSchema(Optimizer::TypeInferencePhaseContext& typeInferencePhaseContext) {

    if (!LogicalBinaryOperatorNode::inferSchema(typeInferencePhaseContext)) {
        return false;
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() != 2) {
        throw TypeInferenceException("JoinLogicalOperatorNode: Found " + std::to_string(distinctSchemas.size())
                                     + " distinct schemas but expected 2 distinct schemas.");
    }

    //reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    //Find the schema for left join key
    FieldAccessExpressionNodePtr leftJoinKey = joinDefinition->getLeftJoinKey();
    auto leftJoinKeyName = leftJoinKey->getFieldName();
    bool fieldExistsInSchema = false;
    for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
        //If field name contains qualifier
        if (leftJoinKeyName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
            fieldExistsInSchema = (*itr)->contains(leftJoinKeyName);
        } else {
            fieldExistsInSchema = ((*itr)->hasFieldName(leftJoinKeyName) != nullptr);
        }
        if (fieldExistsInSchema) {
            leftInputSchema->copyFields(*itr);
            leftJoinKey->inferStamp(typeInferencePhaseContext, leftInputSchema);
            //remove the schema from distinct schema list
            distinctSchemas.erase(itr);
            break;
        }
        itr++;
    }

    if (!fieldExistsInSchema) {
        NES_ERROR("JoinLogicalOperatorNode: Unable to find left join key " + leftJoinKeyName + " in schemas.");
        throw TypeInferenceException("JoinLogicalOperatorNode: Unable to find left join key " + leftJoinKeyName + " in schemas.");
    }

    //Find the schema for right join key
    FieldAccessExpressionNodePtr rightJoinKey = joinDefinition->getRightJoinKey();
    auto rightJoinKeyName = rightJoinKey->getFieldName();
    //If field name contains qualifier
    if (rightJoinKeyName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
        fieldExistsInSchema = distinctSchemas[0]->contains(rightJoinKeyName);
    } else {
        fieldExistsInSchema = (distinctSchemas[0]->hasFieldName(rightJoinKeyName) != nullptr);
    }
    if (fieldExistsInSchema) {
        rightInputSchema->copyFields(distinctSchemas[0]);
        rightJoinKey->inferStamp(typeInferencePhaseContext, rightInputSchema);
    }
    distinctSchemas.clear();

    if (!fieldExistsInSchema) {
        NES_ERROR("JoinLogicalOperatorNode: Unable to find right join key " + rightJoinKeyName + " in schemas.");
        throw TypeInferenceException("JoinLogicalOperatorNode: Unable to find right join key " + rightJoinKeyName
                                     + " in schemas.");
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

    NES_DEBUG("Binary infer left schema=" << leftInputSchema->toString() << " right schema=" << rightInputSchema->toString());
    if (leftInputSchema->getSchemaSizeInBytes() == 0) {
        NES_ERROR("JoinLogicalOperatorNode: left schema is emtpy");
        throw TypeInferenceException("JoinLogicalOperatorNode: Left input schema is not initialized.");
    }

    if (rightInputSchema->getSchemaSizeInBytes() == 0) {
        NES_ERROR("JoinLogicalOperatorNode: right schema is emtpy");
        throw TypeInferenceException("JoinLogicalOperatorNode: Right input schema is not initialized.");
    }

    //Check that both left and right schema should be different
    if (rightInputSchema->equals(leftInputSchema, false)) {
        NES_ERROR("JoinLogicalOperatorNode: Found both left and right input schema to be same.");
        throw TypeInferenceException("JoinLogicalOperatorNode: Found both left and right input schema to be same.");
    }

    //Infer stamp of window definition
    auto windowType = Windowing::WindowType::asTimeBasedWindowType(joinDefinition->getWindowType());
    windowType->inferStamp(leftInputSchema);

    //Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    auto sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    auto sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    auto newQualifierForSystemField = sourceNameLeft + sourceNameRight;
    outputSchema->addField(createField(newQualifierForSystemField + "$start", UINT64));
    outputSchema->addField(createField(newQualifierForSystemField + "$end", UINT64));
    outputSchema->addField(AttributeField::create(newQualifierForSystemField + "$key", leftJoinKey->getStamp()));

    // create dynamic fields to store all fields from left and right sources
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Outputschema for join=" << outputSchema->toString());
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateSourceTypes(leftInputSchema, rightInputSchema);
    return true;
}

OperatorNodePtr JoinLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createJoinOperator(joinDefinition, id);
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
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

bool JoinLogicalOperatorNode::equal(NodePtr const& rhs) const { return rhs->instanceOf<JoinLogicalOperatorNode>(); }// todo

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

std::vector<OriginId> JoinLogicalOperatorNode::getOutputOriginIds() { return OriginIdAssignmentOperator::getOutputOriginIds(); }

}// namespace NES