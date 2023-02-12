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
#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMEXECUTIONCONTEXT_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_BACKENDS_WASM_WASMEXECUTIONCONTEXT_HPP_
#include <iostream>
#include <memory>
#include <utility>
#include <Runtime/WorkerContext.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Nautilus::Backends::WASM {

class WASMExecutionContext {
  public:
    WASMExecutionContext(size_t bl, char qb[]) : binaryLength(bl) {
        queryBinary = qb;
    };
    size_t getBinaryLength() const { return binaryLength; }
    char* getQueryBinary() { return queryBinary; }
    void setArgs(Runtime::WorkerContextPtr wc, Runtime::Execution::PipelineExecutionContextPtr pc, std::shared_ptr<Runtime::TupleBuffer> tb) {
        workerContext = std::move(wc);
        pipelineExeContext = std::move(pc);
        tupleBuffer = std::move(tb);
    }
    Runtime::WorkerContextPtr workerContext = nullptr;
    Runtime::Execution::PipelineExecutionContextPtr pipelineExeContext = nullptr;
    std::shared_ptr<Runtime::TupleBuffer> tupleBuffer = nullptr;

  private:
    size_t binaryLength;
    char* queryBinary;
};

}

#endif