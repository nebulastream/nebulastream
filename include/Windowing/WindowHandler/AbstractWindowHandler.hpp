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

#ifndef INCLUDE_WINDOWS_WINDOW_HPP_
#define INCLUDE_WINDOWS_WINDOW_HPP_

#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Windowing/LogicalWindowDefinition.hpp>
#include <Windowing/Runtime/WindowManager.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Windowing/WindowPolicies/BaseWindowTriggerPolicyDescriptor.hpp>
#include <Windowing/WindowPolicies/ExecutableOnTimeTriggerPolicy.hpp>
#include <Windowing/WindowPolicies/OnTimeTriggerPolicyDescription.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES::Windowing {

/**
 * @brief The abstract window handler is the base class for all window handlers
 */
class AbstractWindowHandler : public detail::virtual_enable_shared_from_this<AbstractWindowHandler, false>,
                              public Runtime::Reconfigurable {
    using inherited0 = detail::virtual_enable_shared_from_this<AbstractWindowHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    explicit AbstractWindowHandler(LogicalWindowDefinitionPtr windowDefinition)
        : windowDefinition(std::move(windowDefinition)), running(false) {
        this->watermarkProcessor = MultiOriginWatermarkProcessor::create(this->windowDefinition->getNumberOfInputEdges());
    }

    ~AbstractWindowHandler() NES_NOEXCEPT(false) override = default;

    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(inherited0::shared_from_this());
    }

    /**
    * @brief Starts thread to check if the window should be triggered.
    * @param stateManager pointer to the current state manager
    * @param localStateVariableId local id of a state on an engine node
    * @return boolean if the window thread is started
    */
    virtual bool start(Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) = 0;

    /**
     * @brief Stops the window thread.
     * @return
     */
    virtual bool stop() = 0;

    /**
     * @brief triggers all ready windows.
     * @return
     */
    virtual void trigger(Runtime::WorkerContextPtr workerContext, bool forceFlush = false) = 0;

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual WindowManagerPtr getWindowManager() = 0;

    virtual std::string toString() = 0;

    LogicalWindowDefinitionPtr getWindowDefinition() { return windowDefinition; }

    /**
     * @brief Gets the last processed watermark
     * @return watermark
     */
    [[nodiscard]] uint64_t getLastWatermark() const { return lastWatermark; }

    /**
     * @brief Sets the last watermark
     * @param lastWatermark
     */
    void setLastWatermark(uint64_t lastWatermark) { this->lastWatermark = lastWatermark; }

    /**
     * @brief Calculate the min watermark
     * @return MinAggregationDescriptor watermark
     */
    uint64_t getMinWatermark() { return watermarkProcessor->getCurrentWatermark(); };

    /**
     * @brief Update the max processed ts, per origin.
     * @param ts
     * @param originId
     */
    void updateMaxTs(uint64_t ts, uint64_t originId, uint64_t sequenceNumber, Runtime::WorkerContextPtr workerContext) {
        std::unique_lock lock(windowMutex);
        NES_DEBUG("updateMaxTs=" << ts << " orId=" << originId);
        if (windowDefinition->getTriggerPolicy()->getPolicyType() == Windowing::triggerOnWatermarkChange) {
            auto oldWatermark = watermarkProcessor->getCurrentWatermark();
            watermarkProcessor->updateWatermark(ts, sequenceNumber, originId);
            auto newWatermark = watermarkProcessor->getCurrentWatermark();
            if (oldWatermark < newWatermark) {
                NES_DEBUG("AbstractWindowHandler trigger for before=" << oldWatermark << " afterMin=" << newWatermark);
                trigger(workerContext);
            }
        } else {
            watermarkProcessor->updateWatermark(ts, sequenceNumber, originId);
        }
    }

    /**
     * @brief This method is necessary to avoid problems with the shared_from_this machinery combined with multi-inheritance
     * @tparam Derived the class type that we want to cast the shared ptr
     * @return this instance casted to the desired shared_ptr<Derived> type
     */
    template<typename Derived>
    std::shared_ptr<Derived> shared_from_base() {
        return std::static_pointer_cast<Derived>(inherited0::shared_from_this());
    }

    /**
     * @brief Reconfigure machinery for the window hander: does nothing in this class but can be overidden in its children
     * @param task
     * @param context
     */
    void reconfigure(Runtime::ReconfigurationMessage& task, Runtime::WorkerContext& context) override {
        Reconfigurable::reconfigure(task, context);
    }

    /**
     * @brief Reconfigure machinery for the window hander: does nothing in this class but can be overidden in its children
     * @param task
     */
    void postReconfigurationCallback(Runtime::ReconfigurationMessage& task) override {
        Reconfigurable::postReconfigurationCallback(task);
    }

  protected:
    mutable std::recursive_mutex windowMutex;
    LogicalWindowDefinitionPtr windowDefinition;
    std::shared_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic_bool running;
    WindowManagerPtr windowManager;
    uint64_t lastWatermark{};
    uint64_t numberOfInputEdges{};
};

}// namespace NES::Windowing
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
