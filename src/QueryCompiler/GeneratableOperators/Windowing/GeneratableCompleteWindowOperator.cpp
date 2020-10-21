#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableCompleteWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableCompleteWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowHandler = createWindowHandler();
    context->setWindow(windowHandler);
    codegen->generateCodeForCompleteWindow(getWindowDefinition(), context);
}
GeneratableWindowOperatorPtr GeneratableCompleteWindowOperator::create(WindowOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableCompleteWindowOperator>(GeneratableCompleteWindowOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableCompleteWindowOperator::GeneratableCompleteWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition) : GeneratableWindowOperator(windowDefinition) {
}

const std::string GeneratableCompleteWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCompleteWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES