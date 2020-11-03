#include <Windowing/WindowPolicies/OnRecordTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnRecordTriggerDescription::create() {
    return std::make_shared<OnRecordTriggerDescription>(OnRecordTriggerDescription());
}

TriggerType OnRecordTriggerDescription::getPolicyType() {
    return this->policy;
}

OnRecordTriggerDescription::OnRecordTriggerDescription()
    : WindowTriggerPolicyDescriptor(triggerOnRecord) {
}

}// namespace NES::Windowing