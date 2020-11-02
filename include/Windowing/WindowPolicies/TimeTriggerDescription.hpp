#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/WindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class TimeTriggerDescription : public WindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create(size_t triggerTimeInMs);

    TriggerType getPolicyType() override;

  protected:
    TimeTriggerDescription(size_t triggerTimeInMs);
    size_t triggerTimeInMs;
};
}
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
