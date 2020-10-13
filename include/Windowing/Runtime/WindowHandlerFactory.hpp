#ifndef NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#define NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
#include <Windowing/Runtime/WindowHandlerImpl.hpp>
namespace NES {
class WindowHandlerFactory {
  public:
    template<class KeyType, class InputType, class FinalAggregateType, class PartialAggregateType>
    static WindowHandlerPtr create(LogicalWindowDefinitionPtr windowDefinition,
                                   QueryManagerPtr queryManager,
                                   BufferManagerPtr bufferManager,
                                   std::shared_ptr<ExecutableWindowAggregation<InputType, FinalAggregateType, PartialAggregateType>> windowAggregation) {
        return std::make_shared<WindowHandlerImpl<KeyType, InputType, FinalAggregateType, PartialAggregateType>>(windowDefinition, queryManager, bufferManager, windowAggregation);
    }
};
}// namespace NES

#endif//NES_INCLUDE_WINDOWING_RUNTIME_WINDOWHANDLERFACTORY_HPP_
