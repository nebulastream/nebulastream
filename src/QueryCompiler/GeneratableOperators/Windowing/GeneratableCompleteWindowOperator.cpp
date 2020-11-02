#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCompleteWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

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
    codegen->generateCodeForCompleteWindow(getWindowDefinition(), generatableWindowAggregation, context);
}
GeneratableWindowOperatorPtr GeneratableCompleteWindowOperator::create(WindowOperatorNodePtr windowLogicalOperator, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id) {
    return std::make_shared<GeneratableCompleteWindowOperator>(GeneratableCompleteWindowOperator(windowLogicalOperator->getWindowDefinition(), std::move(generatableWindowAggregation), id));
}

GeneratableCompleteWindowOperator::GeneratableCompleteWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id)
    : GeneratableWindowOperator(std::move(windowDefinition), std::move(generatableWindowAggregation), id) {}

const std::string GeneratableCompleteWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCompleteWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES