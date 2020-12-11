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
#include <NodeEngine/Execution/ExecutablePipeline.hpp>
#include <State/StateManager.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType, class ValueTypeLeft, class ValueTypeRight>
class JoinHandler : public AbstractJoinHandler {
  public:
    explicit JoinHandler(Join::LogicalJoinDefinitionPtr joinDefinition,
                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                         BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction)
        : AbstractJoinHandler(std::move(joinDefinition), std::move(executablePolicyTrigger)),
          executableJoinAction(std::move(executableJoinAction)) {
        NES_ASSERT(this->joinDefinition, "invalid join definition");
        numberOfInputEdgesRight = this->joinDefinition->getNumberOfInputEdgesRight();
        numberOfInputEdgesLeft = this->joinDefinition->getNumberOfInputEdgesLeft();
        lastWatermarkLeft = 0;
        lastWatermarkRight = 0;
        NES_TRACE("Created join handler");
    }

    static AbstractJoinHandlerPtr
    create(Join::LogicalJoinDefinitionPtr joinDefinition, Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
           BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction) {
        return std::make_shared<JoinHandler>(joinDefinition, executablePolicyTrigger, executableJoinAction);
    }

    virtual ~JoinHandler() { NES_TRACE("~JoinHandler()"); }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override {
        NES_DEBUG("JoinHandler start " << this);
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
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler: run window action " << executableJoinAction->toString());

        if (originIdToMaxTsMapLeft.size() < numberOfInputEdgesLeft
            || originIdToMaxTsMapRight.size() < numberOfInputEdgesRight) {
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
                std::unique_lock innerLock(it.second->mutex());// sorry we have to lock here too :(
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
                std::unique_lock innerLock(it.second->mutex());// sorry we have to lock here too :(
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
        lock.unlock();
        executableJoinAction->doAction(leftJoinState, rightJoinState, watermarkLeft, watermarkRight, lastWatermarkLeft,
                                       lastWatermarkRight);
        lock.lock();

        NES_DEBUG("JoinHandler:  set lastWatermarkLeft to=" << std::max(watermarkLeft, lastWatermarkLeft));
        lastWatermarkLeft = std::max(watermarkLeft, lastWatermarkLeft);
        NES_DEBUG("JoinHandler:  set lastWatermarkRight to=" << std::max(watermarkRight, lastWatermarkRight));
        lastWatermarkRight = std::max(watermarkRight, lastWatermarkRight);
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    void updateMaxTs(uint64_t ts, uint64_t originId, bool isLeftSide) override {
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler: updateAllMaxTs with ts=" << ts << " originId=" << originId);
        if (joinDefinition->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnWatermarkChange) {
            uint64_t beforeMin;
            uint64_t afterMin;
            if (isLeftSide) {
                beforeMin = getMinWatermark(leftSide);
                originIdToMaxTsMapLeft[originId] = std::max(originIdToMaxTsMapLeft[originId], ts);
                afterMin = getMinWatermark(leftSide);

            } else {
                beforeMin = getMinWatermark(rightSide);
                originIdToMaxTsMapRight[originId] = std::max(originIdToMaxTsMapRight[originId], ts);
                afterMin = getMinWatermark(rightSide);
            }

            NES_DEBUG("JoinHandler: updateAllMaxTs with beforeMin=" << beforeMin << " afterMin=" << afterMin);
            if (beforeMin < afterMin) {
                trigger();
            }
        } else {
            if (isLeftSide) {
                originIdToMaxTsMapLeft[originId] = std::max(originIdToMaxTsMapLeft[originId], ts);
            } else {
                originIdToMaxTsMapRight[originId] = std::max(originIdToMaxTsMapRight[originId], ts);
            }
        }
    }

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    bool setup(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override {
        this->originId = 0;

        // Initialize JoinHandler Manager
        this->windowManager = std::make_shared<Windowing::WindowManager>(joinDefinition->getWindowType());
        // Initialize StateVariable
        auto leftDefaultCallback = [](const KeyType&) {
            return new Windowing::WindowedJoinSliceListStore<ValueTypeLeft>();
        };
        auto rightDefaultCallback = [](const KeyType&) {
            return new Windowing::WindowedJoinSliceListStore<ValueTypeRight>();
        };
        this->leftJoinState =
            StateManager::instance().registerStateWithDefault<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>(
                "leftSide", leftDefaultCallback);
        this->rightJoinState =
            StateManager::instance().registerStateWithDefault<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>(
                "rightSide", rightDefaultCallback);

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

  private:
    std::recursive_mutex mutex;

    StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>* leftJoinState;
    StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>* rightJoinState;

    Join::BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
