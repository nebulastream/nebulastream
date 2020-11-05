#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWACTIONDESCRIPTOR_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWACTIONDESCRIPTOR_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

enum ActionType {
    WindowAggregationTriggerAction,
    SliceAggregationTriggerAction
};

class BaseWindowActionDescriptor {
  public:
    /**
     * @brief this function will return the type of the action
     * @return
     */
    virtual ActionType getActionType() = 0;

  protected:
    BaseWindowActionDescriptor(ActionType action);
    ActionType action;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_WINDOWACTIONDESCRIPTOR_HPP_
