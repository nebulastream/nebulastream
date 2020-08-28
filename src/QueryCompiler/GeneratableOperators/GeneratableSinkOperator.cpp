
#include <QueryCompiler/GeneratableOperators/GeneratableSinkOperator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

namespace NES{

void GeneratableSinkOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, context);
}

void GeneratableSinkOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForEmit(context->getResultSchema(), context);
}
GeneratableSinkOperatorPtr GeneratableSinkOperator::create(SinkLogicalOperatorNodePtr sinkLogicalOperatorNode) {
    return std::make_shared<GeneratableSinkOperator>(GeneratableSinkOperator(sinkLogicalOperatorNode->getSinkDescriptor()));
}
GeneratableSinkOperator::GeneratableSinkOperator(SinkDescriptorPtr sinkDescriptor) : SinkLogicalOperatorNode(std::move(sinkDescriptor)) {
}

}