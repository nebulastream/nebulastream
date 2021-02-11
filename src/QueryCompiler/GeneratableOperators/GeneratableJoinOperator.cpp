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
#include <Nodes/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableScanOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/WindowHandler/JoinOperatorHandler.hpp>

namespace NES {

void GeneratableJoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinOperatorHandler = Join::JoinOperatorHandler::create(joinDefinition, context->getResultSchema());

    SchemaPtr inputSchemaLeftChild;
    SchemaPtr inputSchemaRightChild;
    if (getChildren()[0]->instanceOf<UnaryOperatorNode>()) {
        inputSchemaLeftChild = getChildren()[0]->as<UnaryOperatorNode>()->getOutputSchema();
        NES_TRACE("child0 unary schema=" << inputSchemaLeftChild->toString());
    } else {
        inputSchemaLeftChild = getChildren()[0]->as<BinaryOperatorNode>()->getOutputSchema();
        NES_TRACE("child0 binary schema=" << inputSchemaLeftChild->toString());
    }
    NES_ASSERT(inputSchemaLeftChild->getSchemaSizeInBytes() != 0, "left schema empty");

    if (getChildren()[1]->instanceOf<UnaryOperatorNode>()) {
        inputSchemaRightChild = getChildren()[1]->as<UnaryOperatorNode>()->getOutputSchema();
        NES_TRACE("child1 unary schema=" << inputSchemaRightChild->toString());
    } else {
        inputSchemaRightChild = getChildren()[1]->as<BinaryOperatorNode>()->getOutputSchema();
        NES_TRACE("child1 binary schema=" << inputSchemaRightChild->toString());
    }
    NES_ASSERT(inputSchemaRightChild->getSchemaSizeInBytes() != 0, "right schema empty");

    NES_DEBUG(" join definition left=" << joinDefinition->getLeftStreamType()->toString()
                                       << " join definition right=" << joinDefinition->getRightStreamType()->toString()
                                       << " input left=" << inputSchemaLeftChild->toString()
                                       << " input right=" << inputSchemaRightChild->toString()
                                       << " join key right=" << joinDefinition->getRightJoinKey()->getFieldName()
                                       << " join key left=" << joinDefinition->getLeftJoinKey()->getFieldName());

    //check that the right join key is not in the left AND right schema
    NES_ASSERT(!(inputSchemaLeftChild->contains(joinDefinition->getRightJoinKey()->getFieldName())
                 && inputSchemaRightChild->contains(joinDefinition->getRightJoinKey()->getFieldName())),
               "Error both scans contain the same join key for right");

    //check that the left join key is not in the left AND right schema
    NES_ASSERT(!(inputSchemaLeftChild->contains(joinDefinition->getLeftJoinKey()->getFieldName())
                 && inputSchemaRightChild->contains(joinDefinition->getLeftJoinKey()->getFieldName())),
               "Error both scans contain the same join key for left");

    auto newPipelineContext1 = PipelineContext::create();
    newPipelineContext1->arity = PipelineContext::BinaryLeft;
    NES_ASSERT(0 == newPipelineContext1->registerOperatorHandler(joinOperatorHandler), "invalid operator handler index");
    //we have to determine if child 0 or child 1 is the left side of the join by looking at their schemas
    if (inputSchemaLeftChild->contains(joinDefinition->getLeftJoinKey()->getFieldName())) {
        NES_TRACE("Binary left choose child left");
        getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);
    } else if (inputSchemaRightChild->contains(joinDefinition->getLeftJoinKey()->getFieldName())) {
        NES_TRACE("Binary left choose child right");
        getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);
    } else {
        NES_ERROR("no input stream contains left join key");
    }

    auto newPipelineContext2 = PipelineContext::create();
    newPipelineContext2->arity = PipelineContext::BinaryRight;
    NES_ASSERT(0 == newPipelineContext2->registerOperatorHandler(joinOperatorHandler), "invalid operator handler index");
    //we have to determine if child 0 or child 1 is the right side of the join by looking at their schemas
    if (inputSchemaLeftChild->contains(joinDefinition->getRightJoinKey()->getFieldName())) {
        NES_TRACE("Binary right choose child left");
        getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);
    } else if (inputSchemaRightChild->contains(joinDefinition->getRightJoinKey()->getFieldName())) {
        NES_TRACE("Binary right choose child right");
        getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);
    } else {
        NES_ERROR("no input stream contains right join key");
    }

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableJoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinOperatorHandlerIndex = codegen->generateJoinSetup(joinDefinition, context);
    codegen->generateCodeForJoin(joinDefinition, context, joinOperatorHandlerIndex);
}

GeneratableJoinOperatorPtr GeneratableJoinOperator::create(JoinLogicalOperatorNodePtr logicalJoinOperator, OperatorId id) {
    return std::make_shared<GeneratableJoinOperator>(
        GeneratableJoinOperator(logicalJoinOperator->getLeftInputSchema(), logicalJoinOperator->getRightInputSchema(),
                                logicalJoinOperator->getOutputSchema(), logicalJoinOperator->getJoinDefinition(), id));
}

GeneratableJoinOperator::GeneratableJoinOperator(SchemaPtr leftSchema, SchemaPtr rightSchema, SchemaPtr outSchema,
                                                 Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id)
    : JoinLogicalOperatorNode(joinDefinition, id), joinDefinition(joinDefinition) {

    NES_ASSERT(leftSchema, "invalid left schema");
    NES_ASSERT(rightSchema, "invalid right schema");
    NES_ASSERT(outSchema, "invalid out schema");
    setLeftInputSchema(leftSchema);
    setRightInputSchema(rightSchema);
    setOutputSchema(outSchema);
}

const std::string GeneratableJoinOperator::toString() const {
    std::stringstream ss;
    ss << "JOIN_(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES