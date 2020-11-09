#include <Operators/LogicalOperators/JoinLogicalOperatorNode.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableJoinOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>

namespace NES {

void GeneratableJoinOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto joinHandler = Windowing::WindowHandlerFactory::createJoinWindowHandler(joinDefinition);

    auto newPipelineContext1 = PipelineContext::create();
    newPipelineContext1->setJoin(joinHandler);
    newPipelineContext1->isLeftSide = true;
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);

    auto newPipelineContext2 = PipelineContext::create();
    newPipelineContext2->setJoin(joinHandler);
    newPipelineContext2->isLeftSide = false;
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

//    context->setJoin(joinHandler);
    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
//
//    leftJoinpipe => sink
//    rightJoinPipe => sink
//
//    qep
//    source => stage!
//    qep
//        - left source 0
//        - right source 1
//        - sink
}

void GeneratableJoinOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
        codegen->generateCodeForJoin(joinDefinition, context);
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