#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableCentralWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableCentralWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableCentralWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    codegen->generateCodeForCentralWindow(getWindowDefinition(), context);
}
GeneratableWindowOperatorPtr GeneratableCentralWindowOperator::create(WindowLogicalOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableCentralWindowOperator>(GeneratableCentralWindowOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableCentralWindowOperator::GeneratableCentralWindowOperator(WindowDefinitionPtr windowDefinition) : WindowLogicalOperatorNode(windowDefinition) {
}

const std::string GeneratableCentralWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCentralWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES