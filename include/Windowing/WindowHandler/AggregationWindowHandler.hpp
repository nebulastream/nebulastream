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

#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/DistributionCharacteristic.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractWindowHandler.hpp>

namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class AggregationWindowHandler : public AbstractWindowHandler {
  public:
    explicit AggregationWindowHandler(
        LogicalWindowDefinitionPtr windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation,
        BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
        BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction)
        : AbstractWindowHandler(std::move(windowDefinition)), executableWindowAggregation(std::move(windowAggregation)),
          executablePolicyTrigger(std::move(executablePolicyTrigger)), executableWindowAction(std::move(executableWindowAction)) {
        NES_ASSERT(this->windowDefinition, "invalid definition");
        this->numberOfInputEdges = this->windowDefinition->getNumberOfInputEdges();
        this->lastWatermark = 0;
        handlerType = this->windowDefinition->getDistributionType()->toString();
    }

    static auto create(LogicalWindowDefinitionPtr windowDefinition,
                       std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation,
                       BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                       BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction){
       return std::make_shared<AggregationWindowHandler>(windowDefinition, windowAggregation, executablePolicyTrigger, executableWindowAction);
    }

    ~AggregationWindowHandler() {
        NES_DEBUG("~AggregationWindowHandler(" << handlerType << "):  calling destructor");
        stop();
    }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override { return executablePolicyTrigger->start(this->shared_from_this()); }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        executablePolicyTrigger->stop();
        StateManager::instance().unRegisterState("window");
        return true;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "AG:";
        std::string triggerType;
        if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Combining) {
            triggerType = "Combining";
        } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Slicing) {
            triggerType = "Slicing";
        } else if (windowDefinition->getDistributionType()->getType() == DistributionCharacteristic::Merging) {
            triggerType = "Merging";
        } else {
            triggerType = "Complete";
        }
        ss << triggerType;
        return ss.str();
    }

    /**
     * @brief triggers all ready windows.
     */
    void trigger() override {
        NES_DEBUG("AggregationWindowHandler(" << handlerType << "):  run window action " << executableWindowAction->toString()
                                              << " distribution type=" << windowDefinition->getDistributionType()->toString());

        if (originIdToMaxTsMap.size() != numberOfInputEdges) {
            NES_DEBUG("AggregationWindowHandler("
                      << handlerType << "): trigger cannot be applied as we did not get one buffer per edge yet, size="
                      << originIdToMaxTsMap.size() << " numberOfInputEdges=" << numberOfInputEdges);
            return;
        }
        auto watermark = getMinWatermark();

        //In the following, find out the minimal watermark among the buffers/stores to know where
        // we have to start the processing from so-called lastWatermark
        // we cannot use 0 as this will create a lot of unnecessary windows
        if (lastWatermark == 0) {
            uint64_t runningWatermark = std::numeric_limits<uint64_t>::max();

            for (auto& it : windowStateVariable->rangeAll()) {
                auto slices = it.second->getSliceMetadata();
                if (!slices.empty()) {
                    runningWatermark = std::min(runningWatermark, slices[0].getStartTs());
                }
            }

            if (runningWatermark != std::numeric_limits<uint64_t>::max()) {
                lastWatermark = runningWatermark;
                NES_DEBUG("AggregationWindowHandler(" << handlerType
                                                      << "): set lastWatermark to min value of stores=" << lastWatermark);
            } else {
                NES_DEBUG("AggregationWindowHandler(" << handlerType
                                                      << "): as there is no buffer yet in any store, we cannot trigger");
                return;
            }
        }

        NES_DEBUG("AggregationWindowHandler(" << handlerType << "):  run doing with watermark=" << watermark
                                              << " lastWatermark=" << lastWatermark);
        executableWindowAction->doAction(getTypedWindowState(), watermark, lastWatermark);
        NES_DEBUG("AggregationWindowHandler(" << handlerType
                                              << "):  set lastWatermark to=" << std::max(watermark, lastWatermark));
        lastWatermark = std::max(watermark, lastWatermark);
    }

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    bool setup(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override {

        // Initialize AggregationWindowHandler Manager
        this->windowManager = std::make_shared<WindowManager>(windowDefinition->getWindowType());
        // Initialize StateVariable
        this->windowStateVariable =
            &StateManager::instance().registerState<KeyType, WindowSliceStore<PartialAggregateType>*>("window");
        executableWindowAction->setup(pipelineExecutionContext);
        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    WindowManagerPtr getWindowManager() override { return this->windowManager; }

    auto getTypedWindowState() { return windowStateVariable; }

    auto getWindowAction(){
        return executableWindowAction;
    }


  private:
    StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable;
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction;
    std::string handlerType;
};

}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
