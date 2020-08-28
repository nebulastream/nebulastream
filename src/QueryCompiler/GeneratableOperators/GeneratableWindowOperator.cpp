
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForWindow(getWindowDefinition(), context);
}
GeneratableWindowOperatorPtr GeneratableWindowOperator::create(WindowLogicalOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableWindowOperator>(GeneratableWindowOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableWindowOperator::GeneratableWindowOperator(WindowDefinitionPtr windowDefinition) : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string GeneratableWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GENERATABLE_WINDOW(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES