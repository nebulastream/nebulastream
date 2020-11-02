#include <Windowing/WindowPolicies/OnEventTriggerDescription.hpp>

namespace NES::Windowing {

WindowTriggerPolicyPtr OnEventTriggerDescription::create() {
    return std::make_shared<OnEventTriggerDescription>(OnEventTriggerDescription());
}

TriggerType OnEventTriggerDescription::getPolicyType()
{
    return this->policy;
}

OnEventTriggerDescription::OnEventTriggerDescription()
    : WindowTriggerPolicyDescriptor(triggerEachEvent){
}

}// namespace NES::Windowing