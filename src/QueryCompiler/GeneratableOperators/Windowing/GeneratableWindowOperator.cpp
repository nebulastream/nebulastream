
#include <QueryCompiler/GeneratableOperators/Windowing/GeneratableWindowOperator.hpp>
#include <Windowing/Runtime/WindowHandlerFactory.hpp>
namespace NES {

WindowHandlerPtr GeneratableWindowOperator::createWindowHandler() {
    return WindowHandlerFactory::createWindowHandler(windowDefinition);
}
}// namespace NES