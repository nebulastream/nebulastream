
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <Windowing/Runtime/WindowHandlerFactory.hpp>
#include <utility>
namespace NES {

GeneratableWindowOperator::GeneratableWindowOperator(LogicalWindowDefinitionPtr windowDefinition) : WindowLogicalOperatorNode(std::move(windowDefinition)){}

WindowHandlerPtr GeneratableWindowOperator::createWindowHandler() {
    return WindowHandlerFactory::createWindowHandler(windowDefinition);
}
}// namespace NES