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
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
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
        const LogicalWindowDefinitionPtr& windowDefinition,
        std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation,
        BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
        BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction,
        uint64_t id,
        PartialAggregateType partialAggregateInitialValue)
        : AbstractWindowHandler(std::move(windowDefinition)), executableWindowAggregation(std::move(windowAggregation)),
          executablePolicyTrigger(std::move(executablePolicyTrigger)), executableWindowAction(std::move(executableWindowAction)),
          id(id), partialAggregateInitialValue(partialAggregateInitialValue) {
        NES_ASSERT(this->windowDefinition, "invalid definition");
        this->numberOfInputEdges = this->windowDefinition->getNumberOfInputEdges();
        this->lastWatermark = 0;
        handlerType = this->windowDefinition->getDistributionType()->toString();
    }

    static auto
    create(LogicalWindowDefinitionPtr windowDefinition,
           std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> windowAggregation,
           BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger,
           BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction,
           uint64_t id,
           PartialAggregateType partialAggregateInitialValue) {
        return std::make_shared<AggregationWindowHandler>(windowDefinition,
                                                          windowAggregation,
                                                          executablePolicyTrigger,
                                                          executableWindowAction,
                                                          id,
                                                          partialAggregateInitialValue);
    }

    ~AggregationWindowHandler() override {
        NES_DEBUG("~AggregationWindowHandler(" << handlerType << "," << id << ")  finished destructor");
        stop();
    }

    /**
   * @brief Starts thread to check if the window should be triggered
   * @return boolean if the window thread is started
   */
    bool start(Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) override {
        std::unique_lock lock(windowMutex);
        this->stateManager = stateManager;
        auto expected = false;
        StateId stateId = {stateManager->getNodeId(), id, localStateVariableId};

        //Defines a callback to execute every time a new key-value pair is created
        auto defaultCallback = [this](const KeyType&) {
            return new Windowing::WindowSliceStore<PartialAggregateType>(partialAggregateInitialValue);
        };
        this->windowStateVariable =
            stateManager->registerStateWithDefault<KeyType, WindowSliceStore<PartialAggregateType>*>(stateId, defaultCallback);
        if (isRunning.compare_exchange_strong(expected, true)) {
            return executablePolicyTrigger->start(this->AbstractWindowHandler::shared_from_base<AggregationWindowHandler>());
        }
        return false;
    }

    /**
     * @brief Stops the window thread.
     * @return
     */
    bool stop() override {
        NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id << "):  stop called");
        auto expected = true;
        bool result = false;
        if (isRunning.compare_exchange_strong(expected, false)) {
            result = executablePolicyTrigger->stop();
            stateManager->unRegisterState(windowStateVariable);
        }
        NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id << "):  stop result =" << result);
        return result;
    }

    std::string toString() override {
        std::stringstream ss;
        ss << "AggregationWindowHandler(" << handlerType << "," << id << "): ";
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

    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override {
        AbstractWindowHandler::reconfigure(task, context);
    }

    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override {
        AbstractWindowHandler::postReconfigurationCallback(task);

        // TODO switch between soft eos (state is drained) and hard eos (state is truncated)
        switch (task.getType()) {
            case Runtime::SoftEndOfStream: {
                break;
            }
            case Runtime::HardEndOfStream: {
                break;
            }
            default: {
                break;
            }
        }
    }

    /**
     * @brief triggers all ready windows.
     */
    void trigger(Runtime::WorkerContextRef workerContext, bool forceFlush = false) override {
        std::unique_lock lock(windowMutex);
        NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id << "):  run window trigger "
                                              << executableWindowAction->toString()
                                              << " distribution type=" << windowDefinition->getDistributionType()->toString()
                                              << " forceFlush=" << forceFlush);
        uint64_t watermark = 0;
        if (!forceFlush) {
            watermark = getMinWatermark();
        } else {
            watermark = getMinWatermark();
            uint64_t windowSize = 0;
            if (windowDefinition->getWindowType()->isTumblingWindow()) {
                TumblingWindow* window = dynamic_cast<TumblingWindow*>(windowDefinition->getWindowType().get());
                windowSize = window->getSize().getTime();
            } else if (windowDefinition->getWindowType()->isSlidingWindow()) {
                SlidingWindow* window = dynamic_cast<SlidingWindow*>(windowDefinition->getWindowType().get());
                windowSize = window->getSize().getTime();
            } else {
                NES_THROW_RUNTIME_ERROR("AggregationWindowHandler: Undefined Window Type");
            }

            auto allowedLateness = windowManager->getAllowedLateness();
            NES_DEBUG("For flushing maxWatermark = " << watermark << " window size=" << windowSize << " trigger ts ="
                                                     << watermark + windowSize << " allowedLateness=" << allowedLateness);
            watermark = watermark + windowSize + allowedLateness;
        }

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
                NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id
                                                      << "): set lastWatermark to min value of stores=" << lastWatermark);
            } else {
                NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id
                                                      << "): as there is no buffer yet in any store, we cannot trigger");
                return;
            }
        }

        NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id << "):  run doing with watermark=" << watermark
                                              << " lastWatermark=" << lastWatermark);

        executableWindowAction->doAction(getTypedWindowState(), watermark, lastWatermark, workerContext);
        NES_DEBUG("AggregationWindowHandler(" << handlerType << "," << id
                                              << "):  set lastWatermark to=" << std::max(watermark, lastWatermark));
        lastWatermark = std::max(watermark, lastWatermark);

        if (forceFlush) {
            bool expected = true;
            if (isRunning.compare_exchange_strong(expected, false)) {
                executablePolicyTrigger->stop();
            }
        }
    }

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    bool setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) override {
        // Initialize AggregationWindowHandler Manager
        //for agg handler we create a unique id from the
        this->windowManager =
            std::make_shared<WindowManager>(windowDefinition->getWindowType(), windowDefinition->getAllowedLateness(), id);
        executableWindowAction->setup(pipelineExecutionContext);
        return true;
    }

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    WindowManagerPtr getWindowManager() override { return this->windowManager; }

    auto getTypedWindowState() { return windowStateVariable; }

    auto getWindowAction() { return executableWindowAction; }

  private:
    Runtime::StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable{nullptr};
    std::shared_ptr<ExecutableWindowAggregation<InputType, PartialAggregateType, FinalAggregateType>> executableWindowAggregation;
    BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    BaseExecutableWindowActionPtr<KeyType, InputType, PartialAggregateType, FinalAggregateType> executableWindowAction;
    std::string handlerType;
    uint64_t id;
    std::atomic<bool> isRunning{false};
    Runtime::StateManagerPtr stateManager;
    PartialAggregateType partialAggregateInitialValue;
};

}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WINDOWHANDLER_AGGREGATIONWINDOWHANDLER_HPP_
