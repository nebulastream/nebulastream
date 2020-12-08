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

#ifndef NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
#define NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
#include <Windowing/WindowingForwardRefs.hpp>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
namespace NES::Windowing {

template<class KeyType, class InputType, class PartialAggregateType, class FinalAggregateType>
class BaseExecutableWindowAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction(StateVariable<KeyType, WindowSliceStore<PartialAggregateType>*>* windowStateVariable,
                          uint64_t currentWatermark, uint64_t lastWatermark) = 0;

    virtual std::string toString() = 0;

    void setup(NodeEngine::QueryManagerPtr queryManager, BufferManagerPtr bufferManager, NodeEngine::Execution::ExecutablePipelinePtr nextPipeline, uint64_t originId) {
        this->queryManager = queryManager;
        this->bufferManager = bufferManager;
        this->nextPipeline = nextPipeline;
        this->originId = originId;
    }

    SchemaPtr getWindowSchema() { return windowSchema; }

  protected:
    NodeEngine::QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    NodeEngine::Execution::ExecutablePipelinePtr nextPipeline;
    uint64_t originId;
    SchemaPtr windowSchema;
};
}// namespace NES::Windowing

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEWINDOWACTION_HPP_
