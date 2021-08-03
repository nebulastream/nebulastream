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

#ifndef INCLUDE_JOIN_WINDOW_HPP_
#define INCLUDE_JOIN_WINDOW_HPP_

#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalJoinDefinition.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Windowing/WindowPolicies/BaseExecutableWindowTriggerPolicy.hpp>
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

namespace NES::Join {

enum JoinSides { leftSide = 0, rightSide = 1 };

/**
 * @brief The abstract window handler is the base class for all window handlers
 */
class AbstractJoinHandler : public detail::virtual_enable_shared_from_this<AbstractJoinHandler, false>,
                            public Runtime::Reconfigurable {
    using inherited0 = detail::virtual_enable_shared_from_this<AbstractJoinHandler, false>;
    using inherited1 = Runtime::Reconfigurable;

  public:
    explicit AbstractJoinHandler(Join::LogicalJoinDefinitionPtr joinDefinition,
                                 Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger)
        : joinDefinition(std::move(joinDefinition)), executablePolicyTrigger(std::move(executablePolicyTrigger)) {
        // nop
    }

    ~AbstractJoinHandler() NES_NOEXCEPT(false) override = default;

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
    virtual void trigger(bool forceFlush = false) = 0;

    /**
    * @brief Initialises the state of this window depending on the window definition.
    */
    virtual bool setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    /**
     * @brief Returns window manager.
     * @return WindowManager.
     */
    virtual Windowing::WindowManagerPtr getWindowManager() = 0;

    virtual std::string toString() = 0;

    LogicalJoinDefinitionPtr getJoinDefinition() { return joinDefinition; }

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
     * @brief Calculate the min watermark for left or right side for among each input edge
     * @return MinAggregationDescriptor watermark
     */
    uint64_t getMinWatermark(JoinSides side) {
        if (side == leftSide) {
            return watermarkProcessorLeft->getCurrentWatermark();
        } else if (side == rightSide) {
            return watermarkProcessorRight->getCurrentWatermark();
        } else {
            NES_THROW_RUNTIME_ERROR("getNumberOfMappings: invalid side");
            return -1;
        }
    }

    /**
     * @brief updates all maxTs in all stores
     * @param ts
     * @param originId
     */
    virtual void updateMaxTs(WatermarkTs ts, OriginId originId, SequenceNumber sequenceNumber, bool leftSide) = 0;

  protected:
    LogicalJoinDefinitionPtr joinDefinition;
    Windowing::BaseExecutableWindowTriggerPolicyPtr executablePolicyTrigger;
    std::atomic_bool running{false};
    Windowing::WindowManagerPtr windowManager;
    uint64_t originId{};
    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> watermarkProcessorLeft;
    std::shared_ptr<Windowing::MultiOriginWatermarkProcessor> watermarkProcessorRight;
    uint64_t lastWatermark{};
    uint64_t numberOfInputEdgesLeft{};
    uint64_t numberOfInputEdgesRight{};
};

}// namespace NES::Join
#endif /* INCLUDE_WINDOWS_WINDOW_HPP_ */
