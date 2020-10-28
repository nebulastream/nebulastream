#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableSlicingWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>

namespace NES {

void GeneratableSlicingWindowOperator::produce(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    // The window operator is a pipeline breaker -> we create a new pipeline context for the children
    auto newPipelineContext = PipelineContext::create();
    getChildren()[0]->as<GeneratableOperator>()->produce(codegen, newPipelineContext);
    context->addNextPipeline(newPipelineContext);
}

void GeneratableSlicingWindowOperator::consume(CodeGeneratorPtr codegen, PipelineContextPtr context) {
    auto windowHandler = createWindowHandler();
    context->setWindow(windowHandler);
    codegen->generateCodeForSlicingWindow(getWindowDefinition(), context);
}
GeneratableDistributedlWindowSliceCreationOperatorPtr GeneratableSlicingWindowOperator::create(WindowOperatorNodePtr windowLogicalOperatorNode, OperatorId id) {
    return std::make_shared<GeneratableSlicingWindowOperator>(GeneratableSlicingWindowOperator(windowLogicalOperatorNode->getWindowDefinition(), id));
}

GeneratableSlicingWindowOperator::GeneratableSlicingWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : GeneratableWindowOperator(windowDefinition, id) {
}

const std::string GeneratableSlicingWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableSlicingWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES