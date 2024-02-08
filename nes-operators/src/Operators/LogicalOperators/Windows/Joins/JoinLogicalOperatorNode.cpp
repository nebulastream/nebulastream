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
#include  <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <utility>

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

Join::LogicalJoinDefinitionPtr JoinLogicalOperatorNode::getJoinDefinition() const { return joinDefinition; }

bool JoinLogicalOperatorNode::inferSchema() {

    if (!LogicalBinaryOperatorNode::inferSchema()) {
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

    // Finds the join schema that contains the joinKey and returns an iterator to the schema
    auto findSchemaInDinstinctSchemas = [&](FieldAccessExpressionNode& joinKey, SchemaPtr& inputSchema) {
        for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
            bool fieldExistsInSchema;
            const auto joinKeyName = joinKey.getFieldName();
            // If field name contains qualifier
            if (joinKeyName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
                fieldExistsInSchema = (*itr)->contains(joinKeyName);
            } else {
                fieldExistsInSchema = ((*itr)->getField(joinKeyName) != nullptr);
            }

            if (fieldExistsInSchema) {
                inputSchema->copyFields(*itr);
                joinKey.inferStamp( inputSchema);
                distinctSchemas.erase(itr);
                return true;
            }
            ++itr;
        }
        return false;
    };

    //Find the schema for left join key
    const auto leftJoinKey = joinDefinition->getLeftJoinKey();
    const auto leftJoinKeyName = leftJoinKey->getFieldName();
    const auto foundLeftKey = findSchemaInDinstinctSchemas(*leftJoinKey, leftInputSchema);
    NES_ASSERT_THROW_EXCEPTION(foundLeftKey,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: Unable to find left join key " + leftJoinKeyName + " in schemas.");

    //Find the schema for right join key
    const auto rightJoinKey = joinDefinition->getRightJoinKey();
    const auto rightJoinKeyName = rightJoinKey->getFieldName();
    const auto foundRightKey = findSchemaInDinstinctSchemas(*rightJoinKey, rightInputSchema);
    NES_ASSERT_THROW_EXCEPTION(foundRightKey,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: Unable to find right join key " + rightJoinKeyName + " in schemas.");

    // Clearing now the distinct schemas
    distinctSchemas.clear();

    //Check if left and right input schema were correctly identified
    NES_ASSERT_THROW_EXCEPTION(!!leftInputSchema,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: Left input schema is not initialized for left join key "
                                   + leftJoinKeyName);
    NES_ASSERT_THROW_EXCEPTION(!!rightInputSchema,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: Right input schema is not initialized for right join key "
                                   + rightJoinKeyName);

    // Checking if left and right input schema are not empty and are not equal
    NES_ASSERT_THROW_EXCEPTION(leftInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: left schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(rightInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: right schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(!rightInputSchema->equals(leftInputSchema, false),
                               TypeInferenceException,
                               "JoinLogicalOperatorNode: Found both left and right input schema to be same.");

    //Infer stamp of window definition
    const auto windowType = joinDefinition->getWindowType()->as<Windowing::TimeBasedWindowType>();
    windowType->inferStamp(leftInputSchema);

    //Reset output schema and add fields from left and right input schema
    outputSchema->clear();
    const auto& sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    windowStartFieldName = newQualifierForSystemField + "$start";
    windowEndFieldName = newQualifierForSystemField + "$end";
    windowKeyFieldName = newQualifierForSystemField + "$key";
    outputSchema->addField(createField(windowStartFieldName, BasicType::UINT64));
    outputSchema->addField(createField(windowEndFieldName, BasicType::UINT64));
    outputSchema->addField(AttributeField::create(windowKeyFieldName, leftJoinKey->getStamp()));

    // create dynamic fields to store all fields from left and right sources
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Outputschema for join={}", outputSchema->toString());
    joinDefinition->updateOutputDefinition(outputSchema);
    joinDefinition->updateSourceTypes(leftInputSchema, rightInputSchema);
    return true;
}

OperatorNodePtr JoinLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createJoinOperator(joinDefinition, id)->as<JoinLogicalOperatorNode>();
    copy->setLeftInputOriginIds(leftInputOriginIds);
    copy->setRightInputOriginIds(rightInputOriginIds);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setOriginId(originId);
    copy->windowStartFieldName = windowStartFieldName;
    copy->windowEndFieldName = windowEndFieldName;
    copy->windowKeyFieldName = windowKeyFieldName;
    copy->setOperatorState(operatorState);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool JoinLogicalOperatorNode::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<JoinLogicalOperatorNode>()) {
        auto rhsJoin = rhs->as<JoinLogicalOperatorNode>();
        return joinDefinition->getWindowType()->equal(rhsJoin->joinDefinition->getWindowType())
            && joinDefinition->getLeftJoinKey()->equal(rhsJoin->joinDefinition->getLeftJoinKey())
            && joinDefinition->getRightJoinKey()->equal(rhsJoin->joinDefinition->getRightJoinKey())
            && joinDefinition->getOutputSchema()->equals(rhsJoin->joinDefinition->getOutputSchema())
            && joinDefinition->getRightSourceType()->equals(rhsJoin->joinDefinition->getRightSourceType())
            && joinDefinition->getLeftSourceType()->equals(rhsJoin->joinDefinition->getLeftSourceType());
    }
    return false;
}

void JoinLogicalOperatorNode::inferStringSignature() {
    OperatorNodePtr operatorNode = shared_from_this()->as<OperatorNode>();
    NES_TRACE("JoinLogicalOperatorNode: Inferring String signature for {}", operatorNode->toString());
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

const std::vector<OriginId> JoinLogicalOperatorNode::getOutputOriginIds() const {
    return OriginIdAssignmentOperator::getOutputOriginIds();
}

void JoinLogicalOperatorNode::setOriginId(OriginId originId) {
    OriginIdAssignmentOperator::setOriginId(originId);
    joinDefinition->setOriginId(originId);
}

const std::string& JoinLogicalOperatorNode::getWindowStartFieldName() const { return windowStartFieldName; }

const std::string& JoinLogicalOperatorNode::getWindowEndFieldName() const { return windowEndFieldName; }

const std::string& JoinLogicalOperatorNode::getWindowKeyFieldName() const { return windowKeyFieldName; }

void JoinLogicalOperatorNode::setWindowStartEndKeyFieldName(const std::string& windowStartFieldName,
                                                            const std::string& windowEndFieldName,
                                                            const std::string& windowKeyFieldName) {
    this->windowStartFieldName = windowStartFieldName;
    this->windowEndFieldName = windowEndFieldName;
    this->windowKeyFieldName = windowKeyFieldName;
}

}// namespace NES