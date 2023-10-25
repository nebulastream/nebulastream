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

#ifndef NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_JOINHANDLER_HPP_
#define NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_JOINHANDLER_HPP_
#include <Operators/LogicalOperators/Windows/Joins/JoinForwardRefs.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Types/TumblingWindow.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowState.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/ReconfigurationMessage.hpp>
#include <Runtime/WorkerContext.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Util/Core.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Runtime/WindowSliceStore.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/WindowActions/BaseExecutableWindowAction.hpp>
#include <Windowing/WindowHandler/AbstractJoinHandler.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>

namespace NES::Join {
template<class KeyType, class ValueTypeLeft, class ValueTypeRight>
class JoinHandler : public AbstractJoinHandler {
  public:
    explicit JoinHandler(const Join::LogicalJoinDefinitionPtr& joinDefinition,
                         const Windowing::BaseExecutableWindowTriggerPolicyPtr& executablePolicyTrigger,
                         BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction,
                         uint64_t id)
        : AbstractJoinHandler(std::move(joinDefinition), std::move(executablePolicyTrigger)),
          executableJoinAction(std::move(executableJoinAction)), id(id), refCnt(3), isRunning(false) {
        NES_ASSERT(this->joinDefinition, "invalid join definition");
        numberOfInputEdgesRight = this->joinDefinition->getNumberOfInputEdgesRight();
        numberOfInputEdgesLeft = this->joinDefinition->getNumberOfInputEdgesLeft();
        this->watermarkProcessorLeft = Windowing::MultiOriginWatermarkProcessor::create(numberOfInputEdgesLeft);
        this->watermarkProcessorRight = Windowing::MultiOriginWatermarkProcessor::create(numberOfInputEdgesRight);
        lastWatermark = 0;
        NES_TRACE("Created join handler with id={}", id);
    }

    static AbstractJoinHandlerPtr create(Join::LogicalJoinDefinitionPtr joinDefinition,
                                         Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
                                         BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction,
                                         uint64_t id) {
        return std::make_shared<JoinHandler>(joinDefinition, executablePolicyTrigger, executableJoinAction, id);
    }

    ~JoinHandler() override { NES_TRACE("~JoinHandler()"); }

    /**
   * @brief Starts thread to check if the window should be triggered.
   * @param stateManager pointer to the current state manager
   * @param localStateVariableId local id of a state on an engine node
   * @return boolean if the window thread is started
   */
    bool start(Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) override {
        std::unique_lock lock(mutex);
        auto expected = false;

        if (isRunning.compare_exchange_strong(expected, true)) {
            this->stateManager = stateManager;
            StateId leftStateId = {stateManager->getNodeId(), id, localStateVariableId};
            localStateVariableId++;
            StateId rightStateId = {stateManager->getNodeId(), id, localStateVariableId};
            std::stringstream thisAsString;
            thisAsString << this;
            NES_DEBUG("JoinHandler start id={} {}", id, thisAsString.str());
            //Defines a callback to execute every time a new key-value pair is created
            auto leftDefaultCallback = [](const KeyType&) {
                return new Windowing::WindowedJoinSliceListStore<ValueTypeLeft>();
            };
            auto rightDefaultCallback = [](const KeyType&) {
                return new Windowing::WindowedJoinSliceListStore<ValueTypeRight>();
            };
            this->leftJoinState =
                stateManager->registerStateWithDefault<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>(
                    leftStateId,
                    leftDefaultCallback);
            this->rightJoinState =
                stateManager->registerStateWithDefault<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>(
                    rightStateId,
                    rightDefaultCallback);
            return executablePolicyTrigger->start(this->shared_from_base<AbstractJoinHandler>());
        }
        return false;
    }

    /**
     * @brief Stops the window thread.
     * @return Boolean if successful
     */
    bool stop() override {
        std::unique_lock lock(mutex);
        NES_DEBUG("JoinHandler stop id={}: stop", id);
        auto expected = true;
        bool result = false;
        if (isRunning.compare_exchange_strong(expected, false)) {
            result = executablePolicyTrigger->stop();
            // TODO add concept for lifecycle management of state variable
            //            StateManager::instance().unRegisterState(leftJoinState);
            //            StateManager::instance().unRegisterState(rightJoinState);
        }
        return result;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "Joinhandler id=" << id;
        return ss.str();
    }

    /**
    * @brief triggers all ready windows.
    */
    void trigger(Runtime::WorkerContextRef workerContext, bool forceFlush = false) override {
        std::unique_lock lock(mutex);
        NES_TRACE("JoinHandler {}: run window action {} forceFlush={}", id, executableJoinAction->toString(), forceFlush);

        auto watermarkLeft = getMinWatermark(JoinSides::leftSide);
        auto watermarkRight = getMinWatermark(JoinSides::rightSide);

        NES_TRACE("JoinHandler {}: run for watermarkLeft={} watermarkRight={} lastWatermark={}",
                  id,
                  watermarkLeft,
                  watermarkRight,
                  lastWatermark);
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
            NES_TRACE("JoinHandler {}: set lastWatermarkLeft to min value of stores={}", id, lastWatermark);
        }

        NES_TRACE("JoinHandler {}: run doing with watermarkLeft={} watermarkRight={} lastWatermark={}",
                  id,
                  watermarkLeft,
                  watermarkRight,
                  lastWatermark);
        lock.unlock();

        auto minMinWatermark = std::min(watermarkLeft, watermarkRight);
        executableJoinAction->doAction(leftJoinState, rightJoinState, minMinWatermark, lastWatermark, workerContext);
        lock.lock();
        NES_TRACE("JoinHandler {}: set lastWatermarkLeft to={}", id, minMinWatermark);
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
    void updateMaxTs(WatermarkTs ts,
                     OriginId originId,
                     SequenceNumber sequenceNumber,
                     Runtime::WorkerContextRef workerContext,
                     bool isLeftSide) override {
        std::unique_lock lock(mutex);
        std::string side = isLeftSide ? "leftSide" : "rightSide";
        NES_TRACE("JoinHandler {}: updateAllMaxTs with ts={} originId={} side={}", id, ts, originId, side);
        if (joinDefinition->getTriggerPolicy()->getPolicyType() == Windowing::TriggerType::triggerOnWatermarkChange) {
            uint64_t beforeMin = 0;
            uint64_t afterMin = 0;
            if (isLeftSide) {
                beforeMin = getMinWatermark(JoinSides::leftSide);
                watermarkProcessorLeft->updateWatermark(ts, sequenceNumber, originId);
                afterMin = getMinWatermark(JoinSides::leftSide);
            } else {
                beforeMin = getMinWatermark(JoinSides::rightSide);
                watermarkProcessorRight->updateWatermark(ts, sequenceNumber, originId);
                afterMin = getMinWatermark(JoinSides::rightSide);
            }

            NES_TRACE("JoinHandler {}: updateAllMaxTs with beforeMin={}  afterMin={}", id, beforeMin, afterMin);
            if (beforeMin < afterMin) {
                trigger(workerContext);
            }
        } else {
            if (isLeftSide) {
                watermarkProcessorLeft->updateWatermark(ts, sequenceNumber, originId);
            } else {
                watermarkProcessorRight->updateWatermark(ts, sequenceNumber, originId);
            }
        }
    }

    /**
        * @brief Initialises the state of this window depending on the window definition.
        */
    bool setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override {
        this->originId = 0;

        NES_DEBUG("JoinHandler {}: setup Join handler with join def={} string={}",
                  id,
                  joinDefinition,
                  joinDefinition->getOutputSchema()->toString());
        // Initialize JoinHandler Manager
        //TODO: note allowed lateness is currently not supported for windwos
        this->windowManager = std::make_shared<Windowing::WindowManager>(joinDefinition->getWindowType(), 0, id);

        executableJoinAction->setup(pipelineExecutionContext, originId);
        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    Windowing::WindowManagerPtr getWindowManager() override { return this->windowManager; }

    /**
     * @brief Returns left join state.
     * @return leftJoinState.
     */
    Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>* getLeftJoinState() {
        return leftJoinState;
    }

    /**
     * @brief Returns right join state.
     * @return rightJoinState.
     */
    Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>* getRightJoinState() {
        return rightJoinState;
    }

    /**
     * @brief reconfigure machinery for the join handler: do not nothing (for now)
     * @param message
     * @param context
     */
    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override {
        AbstractJoinHandler::reconfigure(message, context);
    }

    /**
     * @brief Reconfiguration machinery on the last thread for the join handler: react to soft or hard termination
     * @param message
     */
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& message) override {
        AbstractJoinHandler::postReconfigurationCallback(message);
        auto flushInflightWindows = [this]() {
            //TODO: this will be removed if we integrate the graceful shutdown
            return;
            // flush in-flight records
            auto windowType = joinDefinition->getWindowType();
            int64_t windowLenghtMs = 0;
            if (windowType->isTimeBasedWindowType()) {
                auto timeBasedWindowType = Windowing::WindowType::asTimeBasedWindowType(windowType);
                if (timeBasedWindowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::TUMBLINGWINDOW) {
                    auto* window = dynamic_cast<Windowing::TumblingWindow*>(windowType.get());
                    windowLenghtMs = window->getSize().getTime();

                } else if (timeBasedWindowType->getTimeBasedSubWindowType() == Windowing::TimeBasedWindowType::SLIDINGWINDOW) {
                    auto* window = dynamic_cast<Windowing::SlidingWindow*>(windowType.get());
                    windowLenghtMs = window->getSlide().getTime();
                } else {
                    NES_THROW_RUNTIME_ERROR("JoinHandler: Undefined Time-Based Window Type");
                }
            } else {
                NES_THROW_RUNTIME_ERROR("JoinHandler: Joins only work for Time-Based Window Type");
            }
            //            NES_DEBUG("Going to flush window {}", toString());
            //            trigger(true);
            //            executableJoinAction->doAction(leftJoinState, rightJoinState, lastWatermark + windowLenghtMs + 1, lastWatermark);
            //            NES_DEBUG("Flushed window content after end of stream message {}", toString());
        };

        auto cleanup = [this]() {
            // drop window content and cleanup resources
            // wait for trigger thread to stop
            stop();
        };

        switch (message.getType()) {
            case Runtime::ReconfigurationType::FailEndOfStream: {
                NES_NOT_IMPLEMENTED();
            }
            case Runtime::ReconfigurationType::SoftEndOfStream: {
                if (refCnt.fetch_sub(1) == 1) {
                    NES_DEBUG("SoftEndOfStream received on join handler {}: going to flush in-flight windows and cleanup",
                              toString());
                    flushInflightWindows();
                    cleanup();
                } else {
                    NES_DEBUG("SoftEndOfStream received on join handler {}: ref counter is: {} ", toString(), refCnt.load());
                }
                break;
            }
            case Runtime::ReconfigurationType::HardEndOfStream: {
                if (refCnt.fetch_sub(1) == 1) {
                    NES_DEBUG("HardEndOfStream received on join handler {} going to flush in-flight windows and cleanup",
                              toString());
                    cleanup();
                } else {
                    NES_DEBUG("HardEndOfStream received on join handler {}: ref counter is:{} ", toString(), refCnt.load());
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
    Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeLeft>*>* leftJoinState;
    Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<ValueTypeRight>*>* rightJoinState;
    Join::BaseExecutableJoinActionPtr<KeyType, ValueTypeLeft, ValueTypeRight> executableJoinAction;
    uint64_t id;
    std::atomic<uint32_t> refCnt;
    Runtime::StateManagerPtr stateManager;
};
}// namespace NES::Join

#endif// NES_CORE_INCLUDE_WINDOWING_WINDOWHANDLER_JOINHANDLER_HPP_
