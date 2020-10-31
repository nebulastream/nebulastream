
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <Windowing/WindowHandler/WindowHandlerFactory.hpp>
#include <utility>

namespace NES {

GeneratableWindowOperator::GeneratableWindowOperator(Windowing::LogicalWindowDefinitionPtr windowDefinition, OperatorId id)
    : WindowLogicalOperatorNode(std::move(windowDefinition), id) {}

Windowing::WindowHandlerPtr GeneratableWindowOperator::createWindowHandler() {
    return Windowing::WindowHandlerFactory::createAggregationWindowHandler(windowDefinition);
}
}// namespace NES