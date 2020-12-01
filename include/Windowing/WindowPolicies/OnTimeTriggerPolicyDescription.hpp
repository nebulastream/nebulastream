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

#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>

namespace NES::Windowing {

class OnTimeTriggerPolicyDescription : public BaseWindowTriggerPolicyDescriptor {
  public:
    static WindowTriggerPolicyPtr create(uint64_t triggerTimeInMs);

    /**
     * @brief method to get the policy type
     * @return
     */
    TriggerType getPolicyType() override;

    /**
    * @brief getter for the time in ms
    * @return time in ms between two triggers
    */
    uint64_t getTriggerTimeInMs() const;

    std::string toString() override;

  protected:
    OnTimeTriggerPolicyDescription(uint64_t triggerTimeInMs);
    uint64_t triggerTimeInMs;
};

typedef std::shared_ptr<OnTimeTriggerPolicyDescription> OnTimeTriggerDescriptionPtr;
}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_TIMETRIGGERDESCRIPTION_HPP_
