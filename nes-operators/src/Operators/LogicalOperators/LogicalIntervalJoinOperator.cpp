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
#include <Common/DataTypes/Integer.hpp>
#include <Exceptions/InvalidFieldException.hpp>
#include <Expressions/BinaryExpressionNode.hpp>
#include <Expressions/FieldAccessExpressionNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/Exceptions/TypeInferenceException.hpp>
#include <Operators/LogicalOperators/LogicalIntervalJoinOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <unordered_set>
#include <utility>

namespace NES {

LogicalIntervalJoinOperator::LogicalIntervalJoinOperator(Join::LogicalIntervalJoinDescriptorPtr intervalJoinDefinition,
                                                         OperatorId id)
    : Operator(id), LogicalBinaryOperator(id), intervalJoinDefinition(std::move(intervalJoinDefinition)) {}

Join::LogicalIntervalJoinDescriptorPtr LogicalIntervalJoinOperator::getIntervalJoinDefinition() const {
    return intervalJoinDefinition;
}

bool LogicalIntervalJoinOperator::equal(NodePtr const& rhs) const {
    if (rhs->instanceOf<LogicalIntervalJoinOperator>()) {
        auto rhsJoin = rhs->as<LogicalIntervalJoinOperator>();

        // extracting the input and output schemas from both IJ operators
        auto lSchema = intervalJoinDefinition->getLeftSchema();
        auto rhsLSchema = rhsJoin->getIntervalJoinDefinition()->getLeftSchema();
        auto rSchema = intervalJoinDefinition->getRightSchema();
        auto rhsRSchema = rhsJoin->getIntervalJoinDefinition()->getRightSchema();
        auto outSchema = intervalJoinDefinition->getOutputSchema();
        auto rhsOutSchema = rhsJoin->getIntervalJoinDefinition()->getOutputSchema();

        // checking if the input and output schemas for both operators are already set
        auto leftSchemasNull = lSchema == nullptr && rhsLSchema == nullptr;
        auto rightSchemasNull = rSchema == nullptr && rhsRSchema == nullptr;
        auto outSchemasNull = outSchema == nullptr && rhsOutSchema == nullptr;
        if (leftSchemasNull) {
            NES_DEBUG("while testing equality of two interval join operators both left schemas were null")
        }
        if (rightSchemasNull) {
            NES_DEBUG("while testing equality of two interval join operators both right schemas were null")
        }
        if (outSchemasNull) {
            NES_DEBUG("while testing equality of two interval join operators both output schemas were null")
        }

        // testing join expression, lower/upper bound and time characteristic for equality
        if (intervalJoinDefinition->getLowerBound() != rhsJoin->intervalJoinDefinition->getLowerBound()
            || intervalJoinDefinition->getUpperBound() != rhsJoin->intervalJoinDefinition->getUpperBound()
            || !(intervalJoinDefinition->getTimeCharacteristic()->equals(
                *rhsJoin->getIntervalJoinDefinition()->getTimeCharacteristic()))
            || !intervalJoinDefinition->getJoinExpression()->equal(rhsJoin->intervalJoinDefinition->getJoinExpression())) {
            return false;
        }

        // testing input and output schemas for equality
        if (!leftSchemasNull
            && (((lSchema == nullptr && rhsLSchema != nullptr) || (lSchema != nullptr && rhsLSchema == nullptr))
                || !(lSchema->equals(rhsLSchema)))) {
            return false;
        }
        if (!rightSchemasNull
            && (((rSchema == nullptr && rhsRSchema != nullptr) || (rSchema != nullptr && rhsRSchema == nullptr))
                || !(rSchema->equals(rhsRSchema)))) {
            return false;
        }
        if (!outSchemasNull
            && (((outSchema == nullptr && rhsOutSchema != nullptr) || (outSchema != nullptr && rhsOutSchema == nullptr))
                || !(outSchema->equals(rhsOutSchema)))) {
            return false;
        }
        return true;
    }
    return false;
}

bool LogicalIntervalJoinOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalIntervalJoinOperator>()->getId() == id;
}

std::string LogicalIntervalJoinOperator::toString() const {
    std::stringstream ss;
    ss << "INTERVALJOIN(" << id << ")";
    return ss.str();
}

bool LogicalIntervalJoinOperator::inferSchema() {

    if (!LogicalBinaryOperator::inferSchema()) {
        return false;
    }

    // validate that only two different type of schemas were present
    if (distinctSchemas.size() != 2) {
        throw TypeInferenceException(
            fmt::format("BinaryOperator: Found {} distinct schemas but expected 2 distinct schemas.", distinctSchemas.size()));
    }

    // reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    // finds the join schema that contains the joinKey and returns an iterator to the schema
    auto findSchemaInDistinctSchemas = [&](FieldAccessExpressionNode& joinKey, const SchemaPtr& inputSchema) {
        for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
            bool fieldExistsInSchema;
            const auto joinKeyName = joinKey.getFieldName();
            // if field name contains qualifier
            if (joinKeyName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) != std::string::npos) {
                fieldExistsInSchema = (*itr)->contains(joinKeyName);
            } else {
                fieldExistsInSchema = ((*itr)->getField(joinKeyName) != nullptr);
            }

            if (fieldExistsInSchema) {
                inputSchema->copyFields(*itr);
                joinKey.inferStamp(inputSchema);
                distinctSchemas.erase(itr);
                return true;
            }
            ++itr;
        }
        return false;
    };

    NES_DEBUG("LogicalIntervalJoinOperator: Iterate over all ExpressionNode to if check join field is in schema.");
    // maintain a list of visited nodes as there are multiple root nodes
    std::unordered_set<std::shared_ptr<BinaryExpressionNode>> visitedExpressions;
    auto bfsIterator = BreadthFirstNodeIterator(intervalJoinDefinition->getJoinExpression());
    for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        if ((*itr)->instanceOf<BinaryExpressionNode>()) {
            auto visitingOp = (*itr)->as<BinaryExpressionNode>();
            if (visitedExpressions.contains(visitingOp)) {
                // skip rest of the steps as the node found in already visited node list
                continue;
            } else {
                visitedExpressions.insert(visitingOp);
                if (!(*itr)->as<BinaryExpressionNode>()->getLeft()->instanceOf<BinaryExpressionNode>()) {
                    // find the schema for left and right join key
                    const auto leftJoinKey = (*itr)->as<BinaryExpressionNode>()->getLeft()->as<FieldAccessExpressionNode>();
                    const auto leftJoinKeyName = leftJoinKey->getFieldName();
                    const auto foundLeftKey = findSchemaInDistinctSchemas(*leftJoinKey, leftInputSchema);
                    NES_ASSERT_THROW_EXCEPTION(foundLeftKey,
                                               TypeInferenceException,
                                               "LogicalIntervalJoinOperator: Unable to find left join key " + leftJoinKeyName
                                                   + " in schemas.");
                    const auto rightJoinKey = (*itr)->as<BinaryExpressionNode>()->getRight()->as<FieldAccessExpressionNode>();
                    const auto rightJoinKeyName = rightJoinKey->getFieldName();
                    const auto foundRightKey = findSchemaInDistinctSchemas(*rightJoinKey, rightInputSchema);
                    NES_ASSERT_THROW_EXCEPTION(foundRightKey,
                                               TypeInferenceException,
                                               "LogicalIntervalJoinOperator: Unable to find right join key " + rightJoinKeyName
                                                   + " in schemas.");

                    NES_DEBUG("LogicalIntervalJoinOperator: Inserting operator in collection of already visited node.");
                    visitedExpressions.insert(visitingOp);
                }
            }
        }
    }
    // clearing now the distinct schemas
    distinctSchemas.clear();

    // checking if left and right input schema are not empty and are not equal
    NES_ASSERT_THROW_EXCEPTION(leftInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "LogicalIntervalJoinOperator: left schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(rightInputSchema->getSchemaSizeInBytes() > 0,
                               TypeInferenceException,
                               "LogicalIntervalJoinOperator: right schema is emtpy");
    NES_ASSERT_THROW_EXCEPTION(!rightInputSchema->equals(leftInputSchema, false),
                               TypeInferenceException,
                               "LogicalIntervalJoinOperator: Found both left and right input schema to be same.");

    // reset output schema and add fields from left and right input schema
    outputSchema->clear();
    const auto& sourceNameLeft = leftInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& sourceNameRight = rightInputSchema->getQualifierNameForSystemGeneratedFields();
    const auto& newQualifierForSystemField = sourceNameLeft + sourceNameRight;

    // add interval start and end fields to output schema
    intervalStartFieldName = newQualifierForSystemField + "$start";
    intervalEndFieldName = newQualifierForSystemField + "$end";
    outputSchema->addField(createField(intervalStartFieldName, BasicType::UINT64));
    outputSchema->addField(createField(intervalEndFieldName, BasicType::UINT64));

    // create dynamic fields to store all fields from left and right streams
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Output schema for  interval join={}", outputSchema->toString());
    intervalJoinDefinition->updateOutputDefinition(outputSchema);
    intervalJoinDefinition->updateIntervalFields(intervalStartFieldName, intervalEndFieldName);
    intervalJoinDefinition->updateInputSchemas(leftInputSchema, rightInputSchema);
    return inferTimeStamp();
}

bool LogicalIntervalJoinOperator::inferTimeStamp() {
    auto timeCharacteristic = intervalJoinDefinition->getTimeCharacteristic();
    if (timeCharacteristic->getType() == Windowing::TimeCharacteristic::Type::EventTime) {
        auto fieldName = timeCharacteristic->getField()->getName();
        auto existingField = leftInputSchema->getField(fieldName);
        if (!existingField->getDataType()->isInteger()) {
            NES_ERROR("IntervalJoin: TimeCharacteristic should use a uint for time field {}", fieldName);
            throw InvalidFieldException("IntervalJoin: TimeCharacteristic should use a uint for time field " + fieldName);
        } else if (existingField) {
            timeCharacteristic->getField()->setName(existingField->getName());
            return true;
        } else if (fieldName == Windowing::TimeCharacteristic::RECORD_CREATION_TS_FIELD_NAME) {
            return true;
        } else {
            NES_ERROR("IntervalJoin: TimeCharacteristic using a non existing time field  {}", fieldName);
            throw InvalidFieldException("IntervalJoin: TimeCharacteristic using a non existing time field " + fieldName);
        }
    }
    return true;
}

OperatorPtr LogicalIntervalJoinOperator::copy() {
    auto copy = LogicalOperatorFactory::createIntervalJoinOperator(intervalJoinDefinition, id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setStatisticId(statisticId);
    for (const auto& [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

void LogicalIntervalJoinOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalIntervalJoinOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "LogicalIntervalJoinOperator: Join should have 2 children.");
    // infer query signatures for child operators
    for (const auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "INTERVALJOIN-DEFINITION(JOINDEFINITION=" << intervalJoinDefinition->toString() << ",";

    auto rightChildSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    // update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES
