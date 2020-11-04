#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWAGGREGATIONTRIGGERACTION_HPP_
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class ExecutableWindowAggregationTriggerAction : public BaseExecutableWindowAction {
  public:
    static BaseExecutableWindowActionPtr create();
    ExecutableWindowAggregationTriggerAction();

    bool doAction() override;

  private:

};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWAGGREGATIONTRIGGERACTION_HPP_
