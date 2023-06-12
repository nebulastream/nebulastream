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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_

#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Nautilus {
class RecordBuffer;
class Operator;
class OperatorState;
class PipelineContext {
  public:
    PipelineContext(Value<MemRef> pipelineContextRef);

    void emitBuffer(const WorkerContext& workerContext, const RecordBuffer& rb);

    uint32_t registerGlobalOperatorState(const Operator* operatorPtr, std::unique_ptr<OperatorState> operatorState);

    Value<MemRef> getGlobalOperatorState(const Operator* tag);

  private:
    Value<MemRef> pipelineContextRef;
    std::unordered_map<const Operator*, uint32_t> operatorIndexMap;
};

}// namespace NES::Nautilus
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_PIPELINECONTEXT_HPP_
