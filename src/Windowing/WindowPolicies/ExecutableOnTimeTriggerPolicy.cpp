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

#include <Util/Logger.hpp>
#include <Util/ThreadNaming.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <memory>

namespace NES::Windowing {

bool ExecutableOnTimeTriggerPolicy::start(AbstractWindowHandlerPtr windowHandler) {
    std::unique_lock lock(runningTriggerMutex);
    if (running) {
        return false;
    }
    running = true;
    std::string triggerType;
    if (windowHandler->getWindowDefinition()->getDistributionType()->getType() == DistributionCharacteristic::Complete || windowHandler->getWindowDefinition()->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
        triggerType = "Combining";
    } else {
        triggerType = "Slicing";
    }

    NES_DEBUG("ExecutableOnTimeTriggerPolicy started thread " << this << " handler=" << windowHandler << " type=" << triggerType << " with ms=" << triggerTimeInMs);
    std::string handlerName = windowHandler->toString();
    thread = std::make_shared<std::thread>([handlerName, windowHandler, this]() {
        setThreadName("whdlr-%d", handlerName.c_str());
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(triggerTimeInMs));
            windowHandler->trigger();
        }
    });
    return true;
}

bool ExecutableOnTimeTriggerPolicy::stop() {
    std::unique_lock lock(runningTriggerMutex);
    NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Stop called");
    if (!running) {
        NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Stop called but was already not running");
        return false;
    }
    running = false;

    if (thread && thread->joinable()) {
        thread->join();
    }
    thread.reset();
    NES_DEBUG("ExecutableOnTimeTriggerPolicy " << this << ": Thread joinded");
    // TODO what happens to the content of the window that it is still in the state?
    return true;
}

ExecutableOnTimeTriggerPolicy::ExecutableOnTimeTriggerPolicy(size_t triggerTimeInMs) : triggerTimeInMs(triggerTimeInMs), running(false), runningTriggerMutex() {
}

ExecutableOnTimeTriggerPtr ExecutableOnTimeTriggerPolicy::create(size_t triggerTimeInMs) {
    return std::make_shared<ExecutableOnTimeTriggerPolicy>(triggerTimeInMs);
}

}// namespace NES::Windowing
