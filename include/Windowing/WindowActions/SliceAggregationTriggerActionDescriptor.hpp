#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_SLICEAGGREGATIONTRIGGERACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_SLICEAGGREGATIONTRIGGERACTION_HPP_
#include <Windowing/WindowActions/BaseWindowActionDescriptor.hpp>

namespace NES::Windowing {

class SliceAggregationTriggerActionDescriptor : public BaseWindowActionDescriptor {
  public:
    static BaseWindowActionDescriptorPtr create();
    ActionType getActionType() override;

  private:
    SliceAggregationTriggerActionDescriptor();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_SLICEAGGREGATIONTRIGGERACTION_HPP_
