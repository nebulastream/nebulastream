#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>

namespace NES::Windowing {

class CompleteAggregationTriggerActionDescriptor : public BaseWindowActionDescriptor {
  public:
    static WindowActionDescriptorPtr create();
    ActionType getActionType() override;

  private:
    CompleteAggregationTriggerActionDescriptor();
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWAGGREGATIONTRIGGERACTION_HPP_
