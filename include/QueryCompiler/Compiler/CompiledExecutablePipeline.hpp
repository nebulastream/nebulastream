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

#ifndef INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_
#define INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_

#include <QueryCompiler/ExecutablePipeline.hpp>
#include <memory>
#include <string>
namespace NES {

class CompiledCode;
typedef std::shared_ptr<CompiledCode> CompiledCodePtr;

/**
 * @brief A executable pipeline that executes compiled code if invoked.
 */
class CompiledExecutablePipeline : public ExecutablePipeline {
  public:
    typedef uint32_t PipelineFunctionReturnType;
    typedef PipelineFunctionReturnType (*PipelineFunctionPtr)(TupleBuffer&, PipelineExecutionContext&, WorkerContextRef);

  public:
    CompiledExecutablePipeline() = delete;

    explicit CompiledExecutablePipeline(PipelineFunctionPtr func);

    explicit CompiledExecutablePipeline(CompiledCodePtr compiled_code);

    static ExecutablePipelinePtr create(CompiledCodePtr compiledCode);

    uint32_t execute(TupleBuffer& inBuffer,
                     QueryExecutionContextPtr context,
                     WorkerContextRef wctx) override;

  private:
    CompiledCodePtr compiledCode;
    PipelineFunctionPtr pipelineFunc;
};
}// namespace NES

#endif//INCLUDE_COMPILED_EXECUTABLE_PIPELINE_HPP_
