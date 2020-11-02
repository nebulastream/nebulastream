#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableMergeOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableMergeOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto newPipelineContext1 = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1);
    auto newPipelineContext2 = PipelineContext::create();
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableMergeOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(outputSchema, context);
}

GeneratableMergeOperatorPtr GeneratableMergeOperator::create(MergeLogicalOperatorNodePtr logicalMergeOperator, OperatorId id) {
    return std::make_shared<GeneratableMergeOperator>(GeneratableMergeOperator(logicalMergeOperator->getOutputSchema(), id));
}

GeneratableMergeOperator::GeneratableMergeOperator(SchemaPtr schemaP, OperatorId id) : MergeLogicalOperatorNode(id) {
    setInputSchema(schemaP);
    setOutputSchema(schemaP);
}

const std::string GeneratableMergeOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_MERGE(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES