#include <Windowing/WindowPolicies/OnWatermarkChangeTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnWatermarkChangeTriggerDescription::create() {
    return std::make_shared<OnWatermarkChangeTriggerDescription>(OnWatermarkChangeTriggerDescription());
}

TriggerType OnWatermarkChangeTriggerDescription::getPolicyType()
{
    return this->policy;
}

OnWatermarkChangeTriggerDescription::OnWatermarkChangeTriggerDescription()
    : WindowTriggerPolicyDescriptor(triggerOnWatermarkChange){
}

}// namespace NES::Windowing