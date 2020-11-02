#include <Windowing/WindowPolicies/TimeTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr TimeTriggerDescription::create(size_t triggerTimeInMs) {
    return std::make_shared<TimeTriggerDescription>(TimeTriggerDescription(std::move(triggerTimeInMs)));
}

TriggerType TimeTriggerDescription::getPolicyType()
{
    return this->policy;
}

TimeTriggerDescription::TimeTriggerDescription(size_t triggerTimeInMs)
    : WindowTriggerPolicyDescriptor(triggerEveryNms),
      triggerTimeInMs(triggerTimeInMs) {
}

}// namespace NES::Windowing