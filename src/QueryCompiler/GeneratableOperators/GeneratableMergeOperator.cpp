
#include <QueryCompiler/GeneratableOperators/GeneratableMergeOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES{

void GeneratableMergeOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) {
    auto newPipelineContext1 = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext1, out);
    auto newPipelineContext2 = PipelineContext::create();
    getChildren()[1]->as<GeneratableOperator>()->produce(codegen, newPipelineContext2, out);

    context->addNextPipeline(newPipelineContext1);
    context->addNextPipeline(newPipelineContext2);
}

void GeneratableMergeOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream&) {
    codegen->generateCodeForEmit(outputSchema, context);
}
GeneratableMergeOperatorPtr GeneratableMergeOperator::create(MergeLogicalOperatorNodePtr) {
    return std::make_shared<GeneratableMergeOperator>(GeneratableMergeOperator());
}

GeneratableMergeOperator::GeneratableMergeOperator(): MergeLogicalOperatorNode() {}

}