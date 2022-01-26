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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_BASEEXECUTABLEWINDOWACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_BASEEXECUTABLEWINDOWACTION_HPP_
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <State/StateManager.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/WindowingForwardRefs.hpp>
namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class BaseExecutableWindowAction {
  public:
    virtual ~BaseExecutableWindowAction() {
        // nop
    }

    /**
     * @brief This function does the action
     * @param windowState
     * @param currentWatermark
     * @param lastWatermark
     * @return bool indicating success
     */
    virtual bool doAction(Runtime::StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable,
                          uint64_t currentWatermark,
                          uint64_t lastWatermark,
                          Runtime::WorkerContextRef workerContext) = 0;

    virtual std::string toString() = 0;

    void emitBuffer(Runtime::TupleBuffer& tupleBuffer) {
        tupleBuffer.setSequenceNumber(++emitSequenceNumber);
        weakExecutionContext.lock()->dispatchBuffer(tupleBuffer);
    };

    void setup(const Runtime::Execution::PipelineExecutionContextPtr& executionContext) {
        this->weakExecutionContext = executionContext;
        this->phantom = executionContext;
    }

  protected:
    std::atomic<uint64_t> emitSequenceNumber = 0;
    std::weak_ptr<Runtime::Execution::PipelineExecutionContext> weakExecutionContext;
    std::shared_ptr<Runtime::Execution::PipelineExecutionContext> phantom;
    SchemaPtr windowSchema;
};
}// namespace NES::Windowing

#endif  // NES_INCLUDE_WINDOWING_WINDOWACTIONS_BASEEXECUTABLEWINDOWACTION_HPP_
