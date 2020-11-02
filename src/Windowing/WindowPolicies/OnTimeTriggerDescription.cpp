#include <Windowing/WindowPolicies/OnTimeTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnTimeTriggerDescription::create(size_t triggerTimeInMs) {
    return std::make_shared<OnTimeTriggerDescription>(OnTimeTriggerDescription(std::move(triggerTimeInMs)));
}

TriggerType OnTimeTriggerDescription::getPolicyType()
{
    return this->policy;
}

size_t OnTimeTriggerDescription::getTriggerTimeInMs() const
{
    return triggerTimeInMs;
}

OnTimeTriggerDescription::OnTimeTriggerDescription(size_t triggerTimeInMs)
    : WindowTriggerPolicyDescriptor(triggerOnTime),
      triggerTimeInMs(triggerTimeInMs) {
}

}// namespace NES::Windowing