#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnTimeTriggerPolicyDescription::create(size_t triggerTimeInMs) {
    return std::make_shared<OnTimeTriggerPolicyDescription>(OnTimeTriggerPolicyDescription(std::move(triggerTimeInMs)));
}

TriggerType OnTimeTriggerPolicyDescription::getPolicyType() {
    return this->policy;
}

size_t OnTimeTriggerPolicyDescription::getTriggerTimeInMs() const {
    return triggerTimeInMs;
}

OnTimeTriggerPolicyDescription::OnTimeTriggerPolicyDescription(size_t triggerTimeInMs)
    : BaseWindowTriggerPolicyDescriptor(triggerOnTime),
      triggerTimeInMs(triggerTimeInMs) {
}

}// namespace NES::Windowing