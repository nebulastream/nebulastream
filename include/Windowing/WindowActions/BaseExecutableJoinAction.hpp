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
#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <State/StateVariable.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Join {

template<class KeyType>
class BaseExecutableJoinAction {
  public:
    /**
     * @brief This function does the action
     * @return bool indicating success
     */
    virtual bool doAction(StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* leftJoinState,
                          StateVariable<KeyType, Windowing::WindowSliceStore<KeyType>*>* rightJoinSate,
                          uint64_t currentWatermarkLeft, uint64_t currentWatermarkRight, uint64_t lastWatermarkLeft,
                          uint64_t lastWatermarkRight) = 0;

    virtual std::string toString() = 0;

    virtual SchemaPtr getJoinSchema() = 0;

    void setup(NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext, uint64_t originId) {
        this->originId = originId;
        this->pipelineExecutionContext = pipelineExecutionContext;
    }

  protected:
    NodeEngine::Execution::PipelineExecutionContextPtr pipelineExecutionContext;
    uint64_t originId;
};
}// namespace NES::Join

#endif//NES_INCLUDE_WINDOWING_WINDOWACTIONS_EXECUTABLEJOIN_HPP_
