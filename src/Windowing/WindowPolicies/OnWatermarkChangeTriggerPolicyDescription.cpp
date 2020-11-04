#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerPolicyDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnWatermarkChangeTriggerPolicyDescription::create() {
    return std::make_shared<OnWatermarkChangeTriggerPolicyDescription>(OnWatermarkChangeTriggerPolicyDescription());
}

TriggerType OnWatermarkChangeTriggerPolicyDescription::getPolicyType() {
    return this->policy;
}

OnWatermarkChangeTriggerPolicyDescription::OnWatermarkChangeTriggerPolicyDescription()
    : BaseWindowTriggerPolicyDescriptor(triggerOnWatermarkChange) {
}

}// namespace NES::Windowing