#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnBufferTriggerDescription : public WindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create();
    TriggerType getPolicyType() override;

  private:
    OnBufferTriggerDescription();
};
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_ONBUFFERTRIGGERDESCRIPTION_HPP_
