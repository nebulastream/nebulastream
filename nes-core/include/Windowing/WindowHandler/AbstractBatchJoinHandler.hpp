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

#ifndef NES_INCLUDE_WINDOWING_WINDOW_HANDLER_ABSTRACT_BATCH_JOIN_HANDLER_HPP_
#define NES_INCLUDE_WINDOWING_WINDOW_HANDLER_ABSTRACT_BATCH_JOIN_HANDLER_HPP_

#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <State/StateManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Windowing/JoinForwardRefs.hpp>
#include <Windowing/LogicalBatchJoinDefinition.hpp>
#include <Windowing/Watermark/MultiOriginWatermarkProcessor.hpp>
//#include <Windowing/WindowingForwardRefs.hpp> todo jm remove
#include <algorithm>
#include <atomic>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <utility>

namespace NES::Join {

/**
 * @brief The AbstractBatchJoinHandler handler is a classes available during compile-time. 
 * Its child class BatchJoinHandler is templated with types at query-run-time and thus only available then.
 */
class AbstractBatchJoinHandler : public detail::virtual_enable_shared_from_this<AbstractBatchJoinHandler, false>,
                            public Runtime::Reconfigurable {
    using inherited0 = detail::virtual_enable_shared_from_this<AbstractBatchJoinHandler, false>; // todo jm no idea what this does
    using inherited1 = Runtime::Reconfigurable;

  public:
    explicit AbstractBatchJoinHandler(Join::LogicalBatchJoinDefinitionPtr batchJoinDefinition)
     : batchJoinDefinition(std::move(batchJoinDefinition)) {
        // nop

    }

    ~AbstractBatchJoinHandler() NES_NOEXCEPT(false) override = default;

    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(inherited0::shared_from_this());
    }

//    todo jm
//    /**
//    * @brief Starts thread to check if the window should be triggered.
//    * @param stateManager pointer to the current state manager
//    * @param localStateVariableId local id of a state on an engine node
//    * @return boolean if the window thread is started
//    */
//    virtual bool start(Runtime::StateManagerPtr stateManager, uint32_t localStateVariableId) = 0;
//
//    /**
//     * @brief Stops the window thread.
//     * @return
//     */
//    virtual bool stop() = 0;
//
//    /**
//    * @brief Initialises the state of this window depending on the window definition.
//    */
//    virtual bool setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) = 0;

    virtual std::string toString() = 0;

    LogicalBatchJoinDefinitionPtr getBatchJoinDefinition() { return batchJoinDefinition; }

    /**
     * @brief This method is necessary to avoid problems with the shared_from_this machinery combined with multi-inheritance
     * @tparam Derived the class type that we want to cast the shared ptr
     * @return this instance casted to the desired shared_ptr<Derived> type
     */
    template<typename Derived>
    std::shared_ptr<Derived> shared_from_base() {
        return std::static_pointer_cast<Derived>(inherited0::shared_from_this());
    }

  protected:
    LogicalBatchJoinDefinitionPtr batchJoinDefinition;
    std::atomic_bool running{false};
    uint64_t originId{};
    /* todo jm
    uint64_t numberOfInputEdgesLeft{};
    uint64_t numberOfInputEdgesRight{};
     */
};

}// namespace NES::Join
#endif// NES_INCLUDE_WINDOWING_WINDOW_HANDLER_ABSTRACT_BATCH_JOIN_HANDLER_HPP_
