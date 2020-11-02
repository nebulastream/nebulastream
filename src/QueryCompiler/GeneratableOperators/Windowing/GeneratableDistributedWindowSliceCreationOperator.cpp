#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableSlicingWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

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
    codegen->generateCodeForSlicingWindow(getWindowDefinition(), generatableWindowAggregation, context);
}
GeneratableDistributedlWindowSliceCreationOperatorPtr GeneratableSlicingWindowOperator::create(WindowOperatorNodePtr windowLogicalOperator, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id) {
    return std::make_shared<GeneratableSlicingWindowOperator>(GeneratableSlicingWindowOperator(windowLogicalOperator->getWindowDefinition(), std::move(generatableWindowAggregation), id));
}

GeneratableSlicingWindowOperator::GeneratableSlicingWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id)
    : GeneratableWindowOperator(std::move(windowDefinition), std::move(generatableWindowAggregation), id) {
}

const std::string GeneratableSlicingWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableSlicingWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES