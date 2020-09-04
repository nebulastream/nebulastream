#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableDistributedlWindowCombinerOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableDistributedlWindowCombinerOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableDistributedlWindowCombinerOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForCentralWindow(getWindowDefinition(), context);
}
GeneratableDistributedlWindowCombinerOperatorPtr GeneratableDistributedlWindowCombinerOperator::create(WindowLogicalOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableDistributedlWindowCombinerOperator>(GeneratableDistributedlWindowCombinerOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableDistributedlWindowCombinerOperator::GeneratableDistributedlWindowCombinerOperator(WindowDefinitionPtr windowDefinition) : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string GeneratableDistributedlWindowCombinerOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableDistributedlWindowCombinerOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES