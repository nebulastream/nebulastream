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

#ifndef NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
#include <State/StateManager.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <NodeEngine/Execution/ExecutablePipeline.hpp>

namespace NES::Join {
template<class KeyType>
class JoinHandler : public AbstractJoinHandler {
  public:
    explicit JoinHandler(Join::LogicalJoinDefinitionPtr joinDefinition,
                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                         BaseExecutableJoinActionPtr<KeyType> executableJoinAction)
        : AbstractJoinHandler(std::move(joinDefinition)), executablePolicyTrigger(executablePolicyTrigger),
          executableJoinAction(executableJoinAction) {
        NES_DEBUG("Construct JoinHandler");
        NES_ASSERT(this->joinDefinition, "wrong join def def");
        numberOfInputEdgesRight = this->joinDefinition->getNumberOfInputEdgesRight();
        numberOfInputEdgesLeft = this->joinDefinition->getNumberOfInputEdgesLeft();
        lastWatermarkLeft = 0;
        lastWatermarkRight = 0;
    }

    static AbstractJoinHandlerPtr create(Join::LogicalJoinDefinitionPtr joinDefinition,
                                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                                         BaseExecutableJoinActionPtr<KeyType> executableJoinAction){
        return std::make_shared<JoinHandler>(joinDefinition, executablePolicyTrigger, executableJoinAction);
    }

    ~JoinHandler() {
        NES_DEBUG("JoinHandler: calling destructor");
        stop();
    }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override {
        NES_DEBUG("JoinHandler start");
        return executablePolicyTrigger->start(this->shared_from_this());
    }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        NES_DEBUG("JoinHandler stop");
        return executablePolicyTrigger->stop();
    }

    std::string toString() override {
        std::stringstream ss;
        //ss << "AG:" << pipelineStageId << +"-" << nextPipeline->getQepParentId();
        return ss.str();
    }

    /**
    * @brief triggers all ready windows.
    */
    void trigger() override {
        NES_DEBUG("JoinHandler: run window action " << executableJoinAction->toString());

        if (originIdToMaxTsMapLeft.size() != numberOfInputEdgesLeft
            && originIdToMaxTsMapRight.size() != numberOfInputEdgesRight) {
            NES_DEBUG(
                "JoinHandler: trigger cannot be applied as we did not get one buffer per edge yet, originIdToMaxTsMapLeft size="
                << originIdToMaxTsMapLeft.size() << " numberOfInputEdgesLeft=" << numberOfInputEdgesLeft
                << " originIdToMaxTsMapRight size=" << originIdToMaxTsMapRight.size()
                << " numberOfInputEdgesRight=" << numberOfInputEdgesRight);
            return;
        } else {
            NES_DEBUG("JoinHandler: trigger applied for size="
                      << originIdToMaxTsMapLeft.size() << " numberOfInputEdgesLeft=" << numberOfInputEdgesLeft
                      << " originIdToMaxTsMapRight size=" << originIdToMaxTsMapRight.size()
                      << " numberOfInputEdgesRight=" << numberOfInputEdgesRight);
        }
        auto watermarkLeft = getMinWatermark(leftSide);
        auto watermarkRight = getMinWatermark(rightSide);

        NES_DEBUG("JoinHandler: run for watermarkLeft=" << watermarkLeft << " watermarkRight=" << watermarkRight
                                                        << " lastWatermarkLeft=" << lastWatermarkLeft
                                                        << " lastWatermarkRight=" << lastWatermarkRight);
        //In the following, find out the minimal watermark among the buffers/stores to know where
        // we have to start the processing from so-called lastWatermark
        // we cannot use 0 as this will create a lot of unnecessary windows
        if (lastWatermarkLeft == 0) {
            lastWatermarkLeft = std::numeric_limits<uint64_t>::max();
            for (auto& it : leftJoinState->rangeAll()) {
                auto slices = it.second->getSliceMetadata();
                if (!slices.empty()) {
                    lastWatermarkLeft = std::min(lastWatermarkLeft, slices[0].getStartTs());
                }
            }
            NES_DEBUG("JoinHandler: set lastWatermarkLeft to min value of stores=" << lastWatermarkLeft);
        }

        if (lastWatermarkRight == 0) {
            lastWatermarkRight = std::numeric_limits<uint64_t>::max();
            for (auto& it : rightJoinState->rangeAll()) {
                auto slices = it.second->getSliceMetadata();
                if (!slices.empty()) {
                    lastWatermarkRight = std::min(lastWatermarkRight, slices[0].getStartTs());
                }
            }
            NES_DEBUG("JoinHandler: set lastWatermarkRight to min value of stores=" << lastWatermarkRight);
        }

        NES_DEBUG("JoinHandler: run doing with watermarkLeft=" << watermarkLeft << " watermarkRight=" << watermarkRight
                                                               << " lastWatermarkLeft=" << lastWatermarkLeft
                                                               << " lastWatermarkRight=" << lastWatermarkRight);
        executableJoinAction->doAction(leftJoinState, rightJoinState, watermarkLeft, watermarkRight, lastWatermarkLeft,
                                       lastWatermarkRight);

        NES_DEBUG("JoinHandler:  set lastWatermarkLeft to=" << std::max(watermarkLeft, lastWatermarkLeft));
        lastWatermarkLeft = std::max(watermarkLeft, lastWatermarkLeft);
        NES_DEBUG("JoinHandler:  set lastWatermarkRight to=" << std::max(watermarkRight, lastWatermarkRight));
        lastWatermarkRight = std::max(watermarkRight, lastWatermarkRight);
    }

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    bool setup(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override {
        this->originId = 0;

        // Initialize JoinHandler Manager
        this->windowManager = std::make_shared<Windowing::WindowManager>(joinDefinition->getWindowType());
        // Initialize StateVariable
        this->leftJoinState = &StateManager::instance().registerState<KeyType, Windowing::WindowSliceStore<KeyType>*>("leftSide");
        this->rightJoinState =
            &StateManager::instance().registerState<KeyType, Windowing::WindowSliceStore<KeyType>*>("rightSide");

        executableJoinAction->setup(pipelineExecutionContext, originId);

        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    Windowing::WindowManagerPtr getWindowManager() override { return this->windowManager; }

    auto getLeftJoinState() { return leftJoinState; }

    auto getRightJoinState() { return rightJoinState; }

    LogicalJoinDefinitionPtr getJoinDefinition() { return joinDefinition; }

  private:
    StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState;
    StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinState;

    //TODO: this will activated once we have a slice store that is capable of handling vectors
    //    StateVariable<KeyType, Windowing::WindowSliceStore<std::vector<ValueTypeLeft>>*>* leftJoinState;
    //    StateVariable<KeyType, Windowing::WindowSliceStore<std::vector<ValueTypeRight>>*>* rightJoinState;

    Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    Join::BaseExecutableJoinActionPtr<KeyType> executableJoinAction;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
