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
#include <NodeEngine/Reconfigurable.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowState.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
#include <Windowing/WindowTypes/TumblingWindow.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {
template<class KeyType, class ValueTypeLeft, class ValueTypeRight>
class JoinHandler : public AbstractJoinHandler {
  public:
    explicit JoinHandler(Join::LogicalJoinDefinitionPtr joinDefinition,
                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                         BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction, uint64_t id)
        : AbstractJoinHandler(std::move(joinDefinition), std::move(executablePolicyTrigger)),
          executableJoinAction(std::move(executableJoinAction)), id(id), refCnt(2), isRunning(false) {
        NES_ASSERT(this->joinDefinition, "invalid join definition");
        numberOfInputEdgesRight = this->joinDefinition->getNumberOfInputEdgesRight();
        numberOfInputEdgesLeft = this->joinDefinition->getNumberOfInputEdgesLeft();
        lastWatermark = 0;
        NES_TRACE("Created join handler with id=" << id);
    }

    static AbstractJoinHandlerPtr create(Join::LogicalJoinDefinitionPtr joinDefinition,
                                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                                         BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction,
                                         uint64_t id) {
        return std::make_shared<JoinHandler>(joinDefinition, executablePolicyTrigger, executableJoinAction, id);
    }

    virtual ~JoinHandler() { NES_TRACE("~JoinHandler()"); }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @return boolean if the window thread is started
   */
    bool start() override {
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler start id=" << id << " " << this);
        auto expected = false;
        if (isRunning.compare_exchange_strong(expected, true)) {
            return executablePolicyTrigger->start(this->shared_from_base<AbstractJoinHandler>());
        }
        return false;
    }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler stop id=" << id << ": stop");
        auto expected = true;
        if (isRunning.compare_exchange_strong(expected, false)) {
            executableJoinAction->destroy();
            return executablePolicyTrigger->stop();
        }
        return false;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "Joinhandler id=" << id;
        return ss.str();
    }

    /**
    * @brief triggers all ready windows.
    */
    void trigger(bool forceFlush = false) override {
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler " << id << ": run window action " << executableJoinAction->toString()
                                 << " forceFlush=" << forceFlush);

        if (originIdToMaxTsMapLeft.size() < numberOfInputEdgesLeft || originIdToMaxTsMapRight.size() < numberOfInputEdgesRight) {
            NES_DEBUG("JoinHandler "
                      << id
                      << ": trigger cannot be applied as we did not get one buffer per edge yet, originIdToMaxTsMapLeft size="
                      << originIdToMaxTsMapLeft.size() << " numberOfInputEdgesLeft=" << numberOfInputEdgesLeft
                      << " originIdToMaxTsMapRight size=" << originIdToMaxTsMapRight.size()
                      << " numberOfInputEdgesRight=" << numberOfInputEdgesRight);
            return;
        } else {
            NES_DEBUG("JoinHandler " << id << ": trigger applied for size=" << originIdToMaxTsMapLeft.size()
                                     << " numberOfInputEdgesLeft=" << numberOfInputEdgesLeft << " originIdToMaxTsMapRight size="
                                     << originIdToMaxTsMapRight.size() << " numberOfInputEdgesRight=" << numberOfInputEdgesRight);
        }
        auto watermarkLeft = getMinWatermark(leftSide);
        auto watermarkRight = getMinWatermark(rightSide);

        NES_DEBUG("JoinHandler " << id << ": run for watermarkLeft=" << watermarkLeft << " watermarkRight=" << watermarkRight
                                 << " lastWatermark=" << lastWatermark);
        //In the following, find out the minimal watermark among the buffers/stores to know where
        // we have to start the processing from so-called lastWatermark
        // we cannot use 0 as this will create a lot of unnecessary windows
        if (lastWatermark == 0) {
            lastWatermark = std::numeric_limits<uint64_t>::max();
            for (auto& it : leftJoinState->rangeAll()) {
                std::unique_lock innerLock(it.second->mutex());// sorry we have to lock here too :(
                auto slices = it.second->getSliceMetadata();
                if (!slices.empty()) {
                    //the first slice is usually the lowest one as they are sorted
                    lastWatermark = std::min(lastWatermark, slices[0].getStartTs());
                }
            }
            for (auto& it : rightJoinState->rangeAll()) {
                std::unique_lock innerLock(it.second->mutex());// sorry we have to lock here too :(
                auto slices = it.second->getSliceMetadata();
                if (!slices.empty()) {
                    //the first slice is usually the lowest one as they are sorted
                    lastWatermark = std::min(lastWatermark, slices[0].getStartTs());
                }
            }
            NES_DEBUG("JoinHandler " << id << ": set lastWatermarkLeft to min value of stores=" << lastWatermark);
        }

        NES_DEBUG("JoinHandler " << id << ": run doing with watermarkLeft=" << watermarkLeft
                                 << " watermarkRight=" << watermarkRight << " lastWatermark=" << lastWatermark);
        lock.unlock();

        auto minMinWatermark = std::min(watermarkLeft, watermarkRight);
        executableJoinAction->doAction(leftJoinState, rightJoinState, minMinWatermark, lastWatermark);
        lock.lock();
        NES_DEBUG("JoinHandler " << id << ": set lastWatermarkLeft to=" << minMinWatermark);
        lastWatermark = minMinWatermark;

        if (forceFlush) {
            bool expected = true;
            if (isRunning.compare_exchange_strong(expected, false)) {
                executablePolicyTrigger->stop();
            }
        }
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    void updateMaxTs(uint64_t ts, uint64_t originId, bool isLeftSide) override {
        std::unique_lock lock(mutex);
        std::string side = isLeftSide ? "leftSide" : "rightSide";
        NES_DEBUG("JoinHandler " << id << ": updateAllMaxTs with ts=" << ts << " originId=" << originId << " side=" << side);
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

            NES_DEBUG("JoinHandler " << id << ": updateAllMaxTs with beforeMin=" << beforeMin << " afterMin=" << afterMin);
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

        NES_DEBUG("JoinHandler " << id << ": setup Join handler with join def=" << joinDefinition
                                 << " string=" << joinDefinition->getOutputSchema()->toString());
        // Initialize JoinHandler Manager
        //TODO: note allowed lateness is currently not supported for windwos
        this->windowManager = std::make_shared<Windowing::WindowManager>(joinDefinition->getWindowType(), 0, id);
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

    /**
     * @brief reconfigure machinery for the join handler: do not nothing (for now)
     * @param message
     * @param context
     */
    void reconfigure(NodeEngine::ReconfigurationMessage& message, NodeEngine::WorkerContext& context) override {
        AbstractJoinHandler::reconfigure(message, context);
    }

    /**
     * @brief Reconfiguration machinery on the last thread for the join handler: react to soft or hard termination
     * @param message
     */
    void postReconfigurationCallback(NodeEngine::ReconfigurationMessage& message) override {
        AbstractJoinHandler::postReconfigurationCallback(message);
        auto flushInflightWindows = [this]() {
            //TODO: this will be removed if we integrate the graceful shutdown
            return;
            // flush in-flight records
            auto windowType = joinDefinition->getWindowType();
            int64_t windowLenghtMs = 0;
            if (windowType->isTumblingWindow()) {
                auto* window = dynamic_cast<Windowing::TumblingWindow*>(windowType.get());
                windowLenghtMs = window->getSize().getTime();

            } else if (windowType->isSlidingWindow()) {
                auto window = dynamic_cast<Windowing::SlidingWindow*>(windowType.get());
                windowLenghtMs = window->getSlide().getTime();
            } else {
                NES_ASSERT(false, "Invalid window");
            }
            NES_DEBUG("Going to flush window " << toString());
            trigger(true);
            executableJoinAction->doAction(leftJoinState, rightJoinState, lastWatermark + windowLenghtMs + 1, lastWatermark);
            NES_DEBUG("Flushed window content after end of stream message " << toString());
        };

        auto cleanup = [this]() {
            // drop window content and cleanup resources
            // wait for trigger thread to stop
            stop();
        };

        switch (message.getType()) {
            case NodeEngine::SoftEndOfStream: {
                if (refCnt.fetch_sub(1) == 1) {
                    NES_DEBUG("SoftEndOfStream received on join handler " << toString()
                                                                          << ": going to flush in-flight windows and cleanup");
                    flushInflightWindows();
                    cleanup();
                } else {
                    NES_DEBUG("SoftEndOfStream received on join handler " << toString() << ": ref counter is: " << refCnt.load());
                }
                break;
            }
            case NodeEngine::HardEndOfStream: {
                if (refCnt.fetch_sub(1) == 1) {
                    NES_DEBUG("HardEndOfStream received on join handler " << toString()
                                                                          << ": going to flush in-flight windows and cleanup");
                    cleanup();
                } else {
                    NES_DEBUG("HardEndOfStream received on join handler " << toString() << ": ref counter is: " << refCnt.load());
                }
                break;
            }
            default: {
                break;
            }
        }
    }

  private:
    std::recursive_mutex mutex;
    std::atomic<bool> isRunning;
    StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>* leftJoinState;
    StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>* rightJoinState;
    Join::BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction;
    uint64_t id;
    std::atomic<uint32_t> refCnt;
};
}// namespace NES::Join
#endif//NES_INCLUDE_WINDOWING_WINDOWHANDLER_JoinHandler_HPP_
