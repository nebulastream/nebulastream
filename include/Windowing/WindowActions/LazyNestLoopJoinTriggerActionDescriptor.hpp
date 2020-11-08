#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_LAZYNESTEDLOOP_HPP
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_LAZYNESTEDLOOP_HPP
#include <Windowing/WindowActions/BaseJoinActionDescriptor.hpp>

namespace NES::Join {

class LazyNestLoopJoinTriggerActionDescriptor : public BaseJoinActionDescriptor {
  public:
    static BaseJoinActionDescriptorPtr create();
    JoinActionType getActionType() override;

  private:
    LazyNestLoopJoinTriggerActionDescriptor();
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_LAZYNESTEDLOOP_HPP
