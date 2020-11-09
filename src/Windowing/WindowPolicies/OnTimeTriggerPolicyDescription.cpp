/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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