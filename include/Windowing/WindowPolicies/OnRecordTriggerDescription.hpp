#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnRecordTriggerDescription : public WindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnRecordTriggerDescription();
};
}
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONEVENTTRIGGERDESCRIPTION_HPP_
