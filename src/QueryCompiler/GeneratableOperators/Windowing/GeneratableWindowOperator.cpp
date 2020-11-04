
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>
#include <utility>

namespace NES {

GeneratableWindowOperator::GeneratableWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, GeneratableWindowAggregationPtr generatableWindowAggregation, OperatorId id)
    : WindowLogicalOperatorNode(std::move(windowDefinition), id), generatableWindowAggregation(generatableWindowAggregation) {}

Windowing::AbstractWindowHandlerPtr GeneratableWindowOperator::createWindowHandler() {
    return Windowing::WindowHandlerFactory::createAggregationWindowHandler(windowDefinition);
}
}// namespace NES