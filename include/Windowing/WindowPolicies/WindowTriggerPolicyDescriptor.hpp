#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICY_HPP_
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

enum TriggerType {
    triggerEveryNms,
    triggerEachEvent,
    triggerEachBuffer,
    triggerEachWatermarkChange
};

class WindowTriggerPolicyDescriptor {
  public:
    /**
     * @brief this function will return the type of the policy
     * @return
     */
    virtual TriggerType getPolicyType() = 0;

  protected:
    WindowTriggerPolicyDescriptor(TriggerType policy);
    TriggerType policy;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICY_HPP_
