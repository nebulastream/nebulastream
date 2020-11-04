#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERPOLICYDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERPOLICYDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnBufferTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnBufferTriggerPolicyDescription();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERPOLICYDESCRIPTION_HPP_
