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

#pragma once

#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime::Execution
{
enum class PipelineStageArity : uint8_t
{
    Unary,
    BinaryLeft,
    BinaryRight
};

/**
 * @brief The executable pipeline stage represents the executable part of a an specific pipeline.
 * For instance, during code generation we generate an implementation of this class, which defines all virtual functions.
 */
class ExecutablePipelineStage
{
public:
    virtual ~ExecutablePipelineStage() = default;

    explicit ExecutablePipelineStage(PipelineStageArity arity = PipelineStageArity::Unary) : arity(arity)
    {
        /// nop
    }

    [[nodiscard]] PipelineStageArity getArity() const { return arity; }

    /**
    * @brief Must be called only once per executable pipeline and initializes the pipeline execution context.
    * e.g, creates the individual operator states -> window handler
    * @param pipelineExecutionContext
    * @return 0 if no error occurred.
    */
    virtual uint32_t setup(PipelineExecutionContext& pipelineExecutionContext);


    /**
    * @brief Is called once per input buffer and performs the computation of each operator.
    * It can access the state in the PipelineExecutionContext and uns the WorkerContext to
    * identify the current worker thread.
    * @param inputTupleBuffer
    * @param pipelineExecutionContext
    * @param workerContext
    * @return 0 if an error occurred.
    */
    virtual ExecutionResult
    execute(Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext)
        = 0;

    /**
     * @brief Must be called exactly once per executable pipeline to remove operator state.
     * @param pipelineExecutionContext
     * @return 0 if no error occurred.
     */
    virtual uint32_t stop(PipelineExecutionContext& pipelineExecutionContext);

private:
    PipelineStageArity arity;
};
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;
}

namespace fmt
{
template <>
struct formatter<NES::Runtime::Execution::ExecutablePipelineStage> : formatter<std::string>
{
    auto format(const NES::Runtime::Execution::ExecutablePipelineStage& ex_pipeline_stage, format_context& ctx) -> decltype(ctx.out())
    {
        return fmt::format_to(ctx.out(), "{}", std::string(magic_enum::enum_name(ex_pipeline_stage.getArity())));
    }
};
}
