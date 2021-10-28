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

#ifndef NES_INCLUDE_WINDOWING_WINDOW_POLICIES_EXECUTABLE_ON_TIME_TRIGGER_POLICY_HPP_
#define NES_INCLUDE_WINDOWING_WINDOW_POLICIES_EXECUTABLE_ON_TIME_TRIGGER_POLICY_HPP_
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <thread>

namespace NES::Windowing {

class ExecutableOnTimeTriggerPolicy : public BaseExecutableWindowTriggerPolicy {
  public:
    explicit ExecutableOnTimeTriggerPolicy(uint64_t triggerTimeInMs);

    static ExecutableOnTimeTriggerPtr create(uint64_t triggerTimeInMs);

    virtual ~ExecutableOnTimeTriggerPolicy();

    /**
     * @brief This function starts the trigger policy
     * @return bool indicating success
     */
    bool start(AbstractWindowHandlerPtr windowHandler, Runtime::WorkerContextPtr workerContext) override;
    bool start(Join::AbstractJoinHandlerPtr joinHandler, Runtime::WorkerContextPtr workerContext) override;

    bool start(AbstractWindowHandlerPtr windowHandler) override;
    bool start(Join::AbstractJoinHandlerPtr joinHandler) override;

    /**
     * @brief This function stop the trigger policy
     * @return bool indicating success
     */
    bool stop() override;

  private:
    std::atomic<bool> running{false};
    std::shared_ptr<std::thread> thread{nullptr};
    std::mutex runningTriggerMutex{};
    uint64_t triggerTimeInMs;
};

}// namespace NES::Windowing
#endif// NES_INCLUDE_WINDOWING_WINDOW_POLICIES_EXECUTABLE_ON_TIME_TRIGGER_POLICY_HPP_
