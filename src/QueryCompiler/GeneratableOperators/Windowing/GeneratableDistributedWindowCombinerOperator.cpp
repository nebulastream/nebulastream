#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableCombiningWindowOperator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <utility>

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
    codegen->generateCodeForCombiningWindow(getWindowDefinition(), generatableWindowAggregation, context);
}

GeneratableDistributedlWindowCombinerOperatorPtr GeneratableCombiningWindowOperator::create(WindowOperatorNodePtr windowLogicalOperator,GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id) {
    return std::make_shared<GeneratableCombiningWindowOperator>(GeneratableCombiningWindowOperator(windowLogicalOperator->getWindowDefinition(), std::move(generatableWindowAggregation), id));
}

GeneratableCombiningWindowOperator::GeneratableCombiningWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition,GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id)
    : GeneratableWindowOperator(std::move(windowDefinition), std::move(generatableWindowAggregation), id) {
}

const std::string GeneratableCombiningWindowOperator::toString() const {
    std::stringstream ss;
    ss << "GeneratableCombiningWindowOperator(" << outputSchema->toString() << ")";
    return ss.str();
}

}// namespace NES