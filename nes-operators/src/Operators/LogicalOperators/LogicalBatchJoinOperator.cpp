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

#include <memory>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES::Experimental
{

LogicalBatchJoinOperator::LogicalBatchJoinOperator(
    std::shared_ptr<Join::Experimental::LogicalBatchJoinDescriptor> batchJoinDefinition, OperatorId id)
    : Operator(id), LogicalBinaryOperator(id), batchJoinDefinition(std::move(batchJoinDefinition))
{
}

bool LogicalBatchJoinOperator::isIdentical(const std::shared_ptr<Node>& rhs) const
{
    return equal(rhs) && NES::Util::as<LogicalBatchJoinOperator>(rhs)->getId() == id;
}

std::string LogicalBatchJoinOperator::toString() const
{
    std::stringstream ss;
    ss << "BATCHJOIN(" << id << ")";
    return ss.str();
}

std::shared_ptr<Join::Experimental::LogicalBatchJoinDescriptor> LogicalBatchJoinOperator::getBatchJoinDefinition() const
{
    return batchJoinDefinition;
}

bool LogicalBatchJoinOperator::inferSchema()
{
    if (!LogicalBinaryOperator::inferSchema())
    {
        return false;
    }

    ///validate that only two different type of schema were present
    if (distinctSchemas.size() != 2)
    {
        throw CannotInferSchema("found {} distinct schemas but expected 2 distinct schemas.", distinctSchemas.size());
    }

    ///reset left and right schema
    leftInputSchema = Schema{leftInputSchema.memoryLayoutType};
    rightInputSchema = Schema{rightInputSchema.memoryLayoutType};

    ///Find the schema for left join key
    const std::shared_ptr<NodeFunctionFieldAccess> buildJoinKey = batchJoinDefinition->getBuildJoinKey();
    auto buildJoinKeyName = buildJoinKey->getFieldName();
    for (auto itr = distinctSchemas.begin(); itr != distinctSchemas.end();)
    {
        if ((*itr).getFieldByName(buildJoinKeyName))
        {
            leftInputSchema.assignToFields(*itr);
            buildJoinKey->inferStamp(leftInputSchema);
            ///remove the schema from distinct schema list
            distinctSchemas.erase(itr);
            break;
        }
        itr++;
    }

    ///Find the schema for right join key
    const std::shared_ptr<NodeFunctionFieldAccess> probeJoinKey = batchJoinDefinition->getProbeJoinKey();
    auto probeJoinKeyName = probeJoinKey->getFieldName();
    for (const auto& schema : distinctSchemas)
    {
        if (schema.getFieldByName(probeJoinKeyName))
        {
            rightInputSchema.assignToFields(schema);
            probeJoinKey->inferStamp(rightInputSchema);
        }
    }

    ///Check that both left and right schema should be different
    if (rightInputSchema == leftInputSchema)
    {
        NES_ERROR("LogicalBatchJoinOperator: Found both left and right input schema to be same.");
        throw CannotInferSchema("LogicalBatchJoinOperator: Found both left and right input schema to be same.");
    }

    NES_DEBUG("Binary infer left schema={} right schema={}", leftInputSchema, rightInputSchema);
    PRECONDITION(leftInputSchema.getSizeOfSchemaInBytes() != 0, "left schema is emtpy");
    PRECONDITION(rightInputSchema.getSizeOfSchemaInBytes() != 0, "right schema is emtpy");

    ///Reset output schema and add fields from left and right input schema
    outputSchema = Schema{outputSchema.memoryLayoutType};

    /// create dynamic fields to store all fields from left and right streams
    for (const auto& field : leftInputSchema.getFields())
    {
        outputSchema.addField(field.name, field.dataType);
    }

    for (const auto& field : rightInputSchema.getFields())
    {
        outputSchema.addField(field.name, field.dataType);
    }

    NES_DEBUG("Output schema for join={}", outputSchema);
    batchJoinDefinition->updateOutputDefinition(outputSchema);
    batchJoinDefinition->updateInputSchemas(leftInputSchema, rightInputSchema);
    return true;
}

std::shared_ptr<Operator> LogicalBatchJoinOperator::copy()
{
    auto copy = std::make_shared<LogicalBatchJoinOperator>(batchJoinDefinition, id);
    copy->setLeftInputSchema(leftInputSchema);
    copy->setRightInputSchema(rightInputSchema);
    copy->setOutputSchema(outputSchema);
    copy->setHashBasedSignature(hashBasedSignature);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}

bool LogicalBatchJoinOperator::equal(const std::shared_ptr<Node>& rhs) const
{
    return NES::Util::instanceOf<LogicalBatchJoinOperator>(rhs);
} /// todo

void LogicalBatchJoinOperator::inferStringSignature()
{
    const std::shared_ptr<Operator> operatorNode = NES::Util::as<Operator>(shared_from_this());
    NES_TRACE("LogicalBatchJoinOperator: Inferring String signature for {}", *operatorNode);
    PRECONDITION(children.size() == 2, "Join should have 2 children but got: {}", children.size());
    ///Infer query signatures for child operators
    for (const auto& child : children)
    {
        const std::shared_ptr<LogicalOperator> childOperator = NES::Util::as<LogicalOperator>(child);
        childOperator->inferStringSignature();
    }
    std::stringstream signatureStream;
    signatureStream << "BATCHJOIN(LEFT-KEY=" << *batchJoinDefinition->getBuildJoinKey() << ",";
    signatureStream << "RIGHT-KEY=" << *batchJoinDefinition->getProbeJoinKey() << ",";

    const auto rightChildSignature = NES::Util::as<LogicalOperator>(children[0])->getHashBasedSignature();
    const auto leftChildSignature = NES::Util::as<LogicalOperator>(children[1])->getHashBasedSignature();
    signatureStream << *rightChildSignature.begin()->second.begin() + ").";
    signatureStream << *leftChildSignature.begin()->second.begin();

    ///Update the signature
    const auto hashCode = hashGenerator(signatureStream.str());
    hashBasedSignature[hashCode] = {signatureStream.str()};
}
}
