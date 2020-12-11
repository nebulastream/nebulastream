#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_WINDOWOPERATORHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_WINDOWOPERATORHANDLER_HPP_

#include <NodeEngine/Execution/OperatorHandler.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
namespace NES::Windowing {

class WindowOperatorHandler : public NodeEngine::Execution::OperatorHandler{
  public:
    WindowOperatorHandler(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema);
    static NodeEngine::Execution::OperatorHandlerPtr create(LogicalWindowDefinitionPtr windowDefinition, SchemaPtr resultSchema);

    void setWindowHandler(AbstractWindowHandlerPtr windowHandler);

    template<template<class, class, class, class> class WindowHandlerType, class KeyType, class InputType,
        class PartialAggregateType, class FinalAggregateType>
    auto getWindowHandler() {
        return std::dynamic_pointer_cast<WindowHandlerType<KeyType, InputType, PartialAggregateType, FinalAggregateType>>(
            windowHandler);
    }

    LogicalWindowDefinitionPtr getWindowDefinition();

    SchemaPtr getResultSchema();
  private:
    LogicalWindowDefinitionPtr windowDefinition;
    AbstractWindowHandlerPtr windowHandler;
    SchemaPtr resultSchema;
};


}

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_WINDOWOPERATORHANDLER_HPP_
