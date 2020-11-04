#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnRecordTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnRecordTriggerPolicyDescription();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
