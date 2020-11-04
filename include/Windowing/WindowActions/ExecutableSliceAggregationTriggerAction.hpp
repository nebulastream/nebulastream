#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class ExecutableSliceAggregationTriggerAction : public BaseExecutableWindowAction {
  public:
    static BaseExecutableWindowActionPtr create();

    bool doAction();

  private:
    ExecutableSliceAggregationTriggerAction();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLESLICEAGGREGATIONTRIGGERACTION_HPP_
