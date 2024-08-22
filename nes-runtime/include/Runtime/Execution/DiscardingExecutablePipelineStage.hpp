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

#include <Runtime/Execution/ExecutablePipelineStage.hpp>

#include <Runtime/ExecutionResult.hpp>

namespace NES
{
namespace Runtime
{
class TupleBuffer;
class WorkerContext;
namespace Execution
{
class PipelineExecutionContext;
} /// namespace Execution
} /// namespace Runtime
} /// namespace NES

namespace NES::Runtime::Execution
{

/**
 * @brief The executable pipeline stage represents the executable part of a an specific pipeline.
 * @Usage this pipeline is the empty pipeline that just retuns without doing processing, during execution, if tasks get invalid, we will switch the
 * function pointer to this one
 * For instance, during code generation we generate an implementation of this class, which defines all virtual functions.
 *
 */
class DiscardingExecutablePipelineStage : public ExecutablePipelineStage
{
public:
    ExecutionResult
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext) override;
};
} /// namespace NES::Runtime::Execution
