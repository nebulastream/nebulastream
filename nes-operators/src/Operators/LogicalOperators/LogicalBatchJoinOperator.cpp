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
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <utility>

namespace NES::Experimental {

LogicalBatchJoinOperator::LogicalBatchJoinOperator(Join::Experimental::LogicalBatchJoinDescriptorPtr batchJoinDefinition,
                                                           OperatorId id)
    : Operator(id), LogicalBinaryOperator(id), batchJoinDefinition(std::move(batchJoinDefinition)) {}

bool LogicalBatchJoinOperator::isIdentical(NodePtr const& rhs) const {
    return equal(rhs) && rhs->as<LogicalBatchJoinOperator>()->getId() == id;
}

std::string LogicalBatchJoinOperator::toString() const {
    std::stringstream ss;
    ss << "BATCHJOIN(" << id << ")";
    return ss.str();
}

Join::Experimental::LogicalBatchJoinDescriptorPtr LogicalBatchJoinOperator::getBatchJoinDefinition() const {
    return batchJoinDefinition;
}

bool LogicalBatchJoinOperator::inferSchema() {

    if (!LogicalBinaryOperator::inferSchema()) {
        return false;
    }

    //validate that only two different type of schema were present
    if (distinctSchemas.size() != 2) {
        throw TypeInferenceException("BinaryOperator: Found " + std::to_string(distinctSchemas.size())
                                     + " distinct schemas but expected 2 distinct schemas.");
    }

    //reset left and right schema
    leftInputSchema->clear();
    rightInputSchema->clear();

    //Find the schema for left join key
    FieldAccessExpressionNodePtr buildJoinKey = batchJoinDefinition->getBuildJoinKey();
    auto buildJoinKeyName = buildJoinKey->getFieldName();
    for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();) {
        if ((*itr)->getField(buildJoinKeyName)) {
            leftInputSchema->copyFields(*itr);
            buildJoinKey->inferStamp( leftInputSchema);
            //remove the schema from distinct schema list
            distinctSchemas.erase(itr);
            break;
        }
        itr++;
    }

    //Find the schema for right join key
    FieldAccessExpressionNodePtr probeJoinKey = batchJoinDefinition->getProbeJoinKey();
    auto probeJoinKeyName = probeJoinKey->getFieldName();
    for (auto& schema : distinctSchemas) {
        if (schema->getField(probeJoinKeyName)) {
            rightInputSchema->copyFields(schema);
            probeJoinKey->inferStamp( rightInputSchema);
        }
    }

    //Check if left input schema was identified
    if (!leftInputSchema) {
        NES_ERROR(
            "LogicalBatchJoinOperator: Left input schema is not initialized. Make sure that left join key is present: {}",
            buildJoinKeyName);
        throw TypeInferenceException("LogicalBatchJoinOperator: Left input schema is not initialized.");
    }

    //Check if right input schema was identified
    if (!rightInputSchema) {
        NES_ERROR(
            "LogicalBatchJoinOperator: Right input schema is not initialized. Make sure that right join key is present: {}",
            probeJoinKeyName);
        throw TypeInferenceException("LogicalBatchJoinOperator: Right input schema is not initialized.");
    }

    //Check that both left and right schema should be different
    if (rightInputSchema->equals(leftInputSchema, false)) {
        NES_ERROR("LogicalBatchJoinOperator: Found both left and right input schema to be same.");
        throw TypeInferenceException("LogicalBatchJoinOperator: Found both left and right input schema to be same.");
    }

    NES_DEBUG("Binary infer left schema={} right schema={}", leftInputSchema->toString(), rightInputSchema->toString());
    NES_ASSERT(leftInputSchema->getSchemaSizeInBytes() != 0, "left schema is emtpy");
    NES_ASSERT(rightInputSchema->getSchemaSizeInBytes() != 0, "right schema is emtpy");

    //Reset output schema and add fields from left and right input schema
    outputSchema->clear();

    // create dynamic fields to store all fields from left and right streams
    for (const auto& field : leftInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    for (const auto& field : rightInputSchema->fields) {
        outputSchema->addField(field->getName(), field->getDataType());
    }

    NES_DEBUG("Output schema for join={}", outputSchema->toString());
    batchJoinDefinition->updateOutputDefinition(outputSchema);
    batchJoinDefinition->updateInputSchemas(leftInputSchema, rightInputSchema);
    return true;
}

OperatorPtr LogicalBatchJoinOperator::copy() {
    auto copy = LogicalOperatorFactory::createBatchJoinOperator(batchJoinDefinition, id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setZ3Signature(z3Signature);
    copy->setHashBasedSignature(hashBasedSignature);
    copy->setStatisticId(statisticId);
    for (auto [key, value] : properties) {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalBatchJoinOperator::equal(NodePtr const& rhs) const {
    return rhs->instanceOf<LogicalBatchJoinOperator>();
}// todo

void LogicalBatchJoinOperator::inferStringSignature() {
    OperatorPtr operatorNode = shared_from_this()->as<Operator>();
    NES_TRACE("LogicalBatchJoinOperator: Inferring String signature for {}", operatorNode->toString());
    NES_ASSERT(!children.empty() && children.size() == 2, "LogicalBatchJoinOperator: Join should have 2 children.");
    //Infer query signatures for child operators
    for (auto& child : children) {
        const LogicalOperatorPtr childOperator = child->as<LogicalOperator>();
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "BATCHJOIN(LEFT-KEY=" << batchJoinDefinition->getBuildJoinKey()->toString() << ",";
    signatureStream << "RIGHT-KEY=" << batchJoinDefinition->getProbeJoinKey()->toString() << ",";

    auto rightChildSignature = children[0]->as<LogicalOperator>()->getHashBasedSignature();
    auto leftChildSignature = children[1]->as<LogicalOperator>()->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    //Update the signature
    auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}// namespace NES::Experimental