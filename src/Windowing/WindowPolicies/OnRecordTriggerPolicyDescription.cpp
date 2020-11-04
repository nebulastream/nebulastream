#include <Windowing/WindowPolicies/OnRecordTriggerPolicyDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnRecordTriggerPolicyDescription::create() {
    return std::make_shared<OnRecordTriggerPolicyDescription>(OnRecordTriggerPolicyDescription());
}

TriggerType OnRecordTriggerPolicyDescription::getPolicyType() {
    return this->policy;
}

OnRecordTriggerPolicyDescription::OnRecordTriggerPolicyDescription()
    : BaseWindowTriggerPolicyDescriptor(triggerOnRecord) {
}

}// namespace NES::Windowing