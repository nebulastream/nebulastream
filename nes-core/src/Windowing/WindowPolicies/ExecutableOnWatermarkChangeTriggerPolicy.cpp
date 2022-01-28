/*
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
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>
#include <Windowing/WindowPolicies/ExecutableOnWatermarkChangeTriggerPolicy.hpp>
#include <memory>

namespace NES::Windowing {

ExecutableOnWatermarkChangeTriggerPolicy::~ExecutableOnWatermarkChangeTriggerPolicy() {
    NES_WARNING("~ExecutableOnWatermarkChangeTriggerPolicy()");
}

bool ExecutableOnWatermarkChangeTriggerPolicy::start(AbstractWindowHandlerPtr, Runtime::WorkerContextRef) {
    //as this policy do not have to start something we immediately return
    return true;
}

bool ExecutableOnWatermarkChangeTriggerPolicy::start(Join::AbstractJoinHandlerPtr, Runtime::WorkerContextRef) {
    //as this policy do not have to start something we immediately return
    return true;
}

bool ExecutableOnWatermarkChangeTriggerPolicy::start(AbstractWindowHandlerPtr) {
    //as this policy do not have to start something we immediately return
    return true;
}

bool ExecutableOnWatermarkChangeTriggerPolicy::start(Join::AbstractJoinHandlerPtr) {
    //as this policy do not have to start something we immediately return
    return true;
}

bool ExecutableOnWatermarkChangeTriggerPolicy::stop() {
    //as this policy do not have to start something we immediately return
    return true;
}

ExecutableOnWatermarkChangeTriggerPolicy::ExecutableOnWatermarkChangeTriggerPolicy() {}

ExecutableOnWatermarkChangeTriggerPolicyPtr ExecutableOnWatermarkChangeTriggerPolicy::create() {
    return std::make_shared<ExecutableOnWatermarkChangeTriggerPolicy>();
}

}// namespace NES::Windowing
