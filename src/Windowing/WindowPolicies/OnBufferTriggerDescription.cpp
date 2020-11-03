#include <Windowing/WindowPolicies/OnBufferTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnBufferTriggerDescription::create() {
    return std::make_shared<OnBufferTriggerDescription>(OnBufferTriggerDescription());
}

TriggerType OnBufferTriggerDescription::getPolicyType() {
    return this->policy;
}

OnBufferTriggerDescription::OnBufferTriggerDescription()
    : WindowTriggerPolicyDescriptor(triggerOnBuffer) {
}

}// namespace NES::Windowing