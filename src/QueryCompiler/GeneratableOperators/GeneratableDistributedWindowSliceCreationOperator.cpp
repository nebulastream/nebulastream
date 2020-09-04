#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableDistributedlWindowSliceCreationOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableDistributedlWindowSliceCreationOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableDistributedlWindowSliceCreationOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForCentralWindow(getWindowDefinition(), context);
}
GeneratableDistributedlWindowSliceCreationOperatorPtr GeneratableDistributedlWindowSliceCreationOperator::create(WindowLogicalOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableDistributedlWindowSliceCreationOperator>(GeneratableDistributedlWindowSliceCreationOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableDistributedlWindowSliceCreationOperator::GeneratableDistributedlWindowSliceCreationOperator(WindowDefinitionPtr windowDefinition) : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string GeneratableDistributedlWindowSliceCreationOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableDistributedlWindowSliceCreationOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES