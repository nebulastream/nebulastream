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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/Runtime/WindowedJoinSliceListStore.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {

template<class KeyType, class InputTypeLeft, class InputTypeRight>
class BaseExecutableJoinAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction(Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeLeft>*>* leftJoinState,
                          Runtime::StateVariable<KeyType, Windowing::WindowedJoinSliceListStore<InputTypeRight>*>* rightJoinSate,
                          uint64_t currentWatermark,
                          uint64_t lastWatermark,
                          Runtime::WorkerContextPtr workerContext) = 0;

    virtual std::string toString() = 0;

    void emitBuffer(Runtime::TupleBuffer& tupleBuffer) {
        tupleBuffer.setSequenceNumber(++emitSequenceNumber);
        weakExecutionContext.lock()->dispatchBuffer(tupleBuffer);
    };

    virtual SchemaPtr getJoinSchema() = 0;

    virtual void setup(Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint64_t originId) {
        NES_ASSERT(pipelineExecutionContext, "invalid pipelineExecutionContext");
        this->originId = originId;
        this->weakExecutionContext = pipelineExecutionContext;
        this->phantom = pipelineExecutionContext;
    }

  protected:
    std::atomic<uint64_t> emitSequenceNumber = 0;
    std::weak_ptr<Runtime::Execution::PipelineExecutionContext> weakExecutionContext;
    // sorry i need to do this to remind us of this hack
    // otherwise we ll file an issue and forget about it
    std::shared_ptr<Runtime::Execution::PipelineExecutionContext> phantom;

    uint64_t originId;
};
}// namespace NES::Join

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
