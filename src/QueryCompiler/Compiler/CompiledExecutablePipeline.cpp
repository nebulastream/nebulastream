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

#include <QueryCompiler/Compiler/CompiledCode.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <utility>

namespace NES {

// TODO this might change across OS
#if defined(__linux__)
static constexpr auto mangledEntryPoint = "_Z14compiled_queryRN3NES11TupleBufferERNS_24PipelineExecutionContextERNS_13WorkerContextE";
#else
#error "unsupported platform/OS"
#endif

CompiledExecutablePipeline::CompiledExecutablePipeline(CompiledCodePtr compiled_code)
    : ExecutablePipeline(false), compiledCode(std::move(compiled_code)), pipelineFunc(compiledCode->getFunctionPointer<PipelineFunctionPtr>(mangledEntryPoint)) {
    // nop
}

CompiledExecutablePipeline::CompiledExecutablePipeline(PipelineFunctionPtr func) : ExecutablePipeline(true), compiledCode(nullptr), pipelineFunc(func) {
    // nop
}

uint32_t CompiledExecutablePipeline::execute(TupleBuffer& inputBuffer,
                                             QueryExecutionContextPtr context,
                                             WorkerContextRef wctx) {
    return (*pipelineFunc)(inputBuffer, *context.get(), wctx);
}

ExecutablePipelinePtr CompiledExecutablePipeline::create(const CompiledCodePtr compiledCode) {
    return std::make_shared<CompiledExecutablePipeline>(compiledCode);
}

}// namespace NES