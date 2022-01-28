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

#include <QueryCompiler/Compiler/LazyCompiledExecutablePipelineStage.hpp>
#include <Util/Logger.hpp>
namespace NES {
NES::LazyCompiledExecutablePipelineStage::LazyCompiledExecutablePipelineStage(
    std::shared_future<Runtime::Execution::ExecutablePipelineStagePtr> futurePipelineStage)
    : futurePipelineStage(futurePipelineStage) {}

uint32_t
NES::LazyCompiledExecutablePipelineStage::setup(NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    return futurePipelineStage.get()->setup(pipelineExecutionContext);
}
uint32_t
NES::LazyCompiledExecutablePipelineStage::start(NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    return futurePipelineStage.get()->start(pipelineExecutionContext);
}
uint32_t
NES::LazyCompiledExecutablePipelineStage::open(NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                               NES::Runtime::WorkerContext& workerContext) {
    return futurePipelineStage.get()->open(pipelineExecutionContext, workerContext);
}
NES::ExecutionResult
NES::LazyCompiledExecutablePipelineStage::execute(NES::Runtime::TupleBuffer& inputTupleBuffer,
                                                  NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                  NES::Runtime::WorkerContext& workerContext) {
    return futurePipelineStage.get()->execute(inputTupleBuffer, pipelineExecutionContext, workerContext);
}
uint32_t
NES::LazyCompiledExecutablePipelineStage::close(NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                                                NES::Runtime::WorkerContext& workerContext) {
    return futurePipelineStage.get()->close(pipelineExecutionContext, workerContext);
}
uint32_t
NES::LazyCompiledExecutablePipelineStage::stop(NES::Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext) {
    return futurePipelineStage.get()->stop(pipelineExecutionContext);
}
std::string NES::LazyCompiledExecutablePipelineStage::getCodeAsString() { return futurePipelineStage.get()->getCodeAsString(); }
Runtime::Execution::ExecutablePipelineStagePtr
LazyCompiledExecutablePipelineStage::create(std::shared_future<Runtime::Execution::ExecutablePipelineStagePtr> futurePipeline) {
    return std::make_shared<LazyCompiledExecutablePipelineStage>(futurePipeline);
}
LazyCompiledExecutablePipelineStage::~LazyCompiledExecutablePipelineStage() {
    NES_DEBUG("~LazyCompiledExecutablePipelineStage()");
}

}// namespace NES