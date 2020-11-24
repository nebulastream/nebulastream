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

#ifndef NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <memory>
#include <mutex>
#include <thread>

namespace NES::Windowing {

class ExecutableOnTimeTriggerPolicy : public BaseExecutableWindowTriggerPolicy {
  public:
    ExecutableOnTimeTriggerPolicy(uint64_t triggerTimeInMs);

    static ExecutableOnTimeTriggerPtr create(uint64_t triggerTimeInMs);

    /**
     * @brief This function starts the trigger policy
     * @return bool indicating success
     */
    bool start(AbstractWindowHandlerPtr windowHandler) override;
    //TODO maybe we can solve this better by having a common parent class for both handler
    bool start(Join::AbstractJoinHandlerPtr joinHandler) override;

    /**
     * @brief This function stop the trigger policy
     * @return bool indicating success
     */
    bool stop() override;

  private:
    bool running;
    std::shared_ptr<std::thread> thread;
    std::mutex runningTriggerMutex;
    uint64_t triggerTimeInMs;
};

}// namespace NES::Windowing
#endif//NES_INCLUDE_WINDOWING_WINDOWPOLICIES_EXECUTABLEONTIMETRIGGERPOLICY_HPP_
