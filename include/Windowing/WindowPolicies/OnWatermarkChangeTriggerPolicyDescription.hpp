#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERPOLICYDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERPOLICYDESCRIPTION_HPP_

#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnWatermarkChangeTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnWatermarkChangeTriggerPolicyDescription();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERPOLICYDESCRIPTION_HPP_
