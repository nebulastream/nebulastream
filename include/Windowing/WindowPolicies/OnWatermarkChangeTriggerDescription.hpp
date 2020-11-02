#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERDESCRIPTION_HPP_

#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnWatermarkChangeTriggerDescription : public WindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnWatermarkChangeTriggerDescription();
};
}
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONWATERMARKCHANGETRIGGERDESCRIPTION_HPP_
