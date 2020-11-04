#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>

namespace NES::Windowing {

class WindowAggregationTriggerActionDescriptor : public BaseWindowActionDescriptor {
  public:
    static BaseWindowActionDescriptorPtr create();
    ActionType getActionType() override;

  private:
    WindowAggregationTriggerActionDescriptor();
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
