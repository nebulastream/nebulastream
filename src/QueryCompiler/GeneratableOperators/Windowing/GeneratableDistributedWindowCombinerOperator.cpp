#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCombiningWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableCombiningWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableCombiningWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowHandler = createWindowHandler();
    context->setWindow(windowHandler);
    codegen->generateCodeForCombiningWindow(getWindowDefinition(), context);
}
GeneratableDistributedlWindowCombinerOperatorPtr GeneratableCombiningWindowOperator::create(WindowLogicalOperatorNodePtr windowLogicalOperatorNode) {
    return std::make_shared<GeneratableCombiningWindowOperator>(GeneratableCombiningWindowOperator(windowLogicalOperatorNode->getWindowDefinition()));
}

GeneratableCombiningWindowOperator::GeneratableCombiningWindowOperator(LogicalWindowDefinitionPtr windowDefinition) : GeneratableWindowOperator(windowDefinition) {
}

const std::string GeneratableCombiningWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCombiningWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES