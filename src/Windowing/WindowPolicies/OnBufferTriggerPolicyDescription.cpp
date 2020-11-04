#include <Windowing/WindowPolicies/OnBufferTriggerPolicyDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnBufferTriggerPolicyDescription::create() {
    return std::make_shared<OnBufferTriggerPolicyDescription>(OnBufferTriggerPolicyDescription());
}

TriggerType OnBufferTriggerPolicyDescription::getPolicyType() {
    return this->policy;
}

OnBufferTriggerPolicyDescription::OnBufferTriggerPolicyDescription()
    : BaseWindowTriggerPolicyDescriptor(triggerOnBuffer) {
}

}// namespace NES::Windowing