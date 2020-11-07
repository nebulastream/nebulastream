#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>

namespace NES {

void GeneratableJoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto newPipelineContext1 = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);
    auto newPipelineContext2 = PipelineContext::create();
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableJoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinHandler = Windowing::WindowHandlerFactory::createAggregationWindowHandler(joinDefinition);
    context->setWindow(joinHandler);
    codegen->generateCodeForSlicingWindow(getWindowDefinition(), generatableWindowAggregation, context);

}

GeneratableJoinOperatorPtr GeneratableJoinOperator::create(JoinLogicalOperatorNodePtr logicalJoinOperator, OperatorId id) {
    return std::make_shared<GeneratableJoinOperator>(GeneratableJoinOperator(logicalJoinOperator->getOutputSchema(), logicalJoinOperator->getJoinDefinition(), id));
}

GeneratableJoinOperator::GeneratableJoinOperator(SchemaPtr schemaP, Join::LogicalJoinDefinitionPtr joinDefinition, OperatorId id) : JoinLogicalOperatorNode(joinDefinition, id), joinDefinition(joinDefinition) {
    setInputSchema(schemaP);
    setOutputSchema(schemaP);

}

const std::string GeneratableJoinOperator::toString() const {
    std::stringstream ss;
    ss << "JOIN_(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES