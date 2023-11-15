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

#ifndef NES_EXECUTABLEOPENCLOPERATOR_H
#define NES_EXECUTABLEOPENCLOPERATOR_H

#include <QueryCompiler/Operators/ExecutableOperator.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>

typedef struct _cl_platform_id* cl_platform_id;
typedef struct _cl_device_id* cl_device_id;
typedef struct _cl_context* cl_context;
typedef struct _cl_command_queue* cl_command_queue;
typedef struct _cl_mem* cl_mem;
typedef struct _cl_program* cl_program;
typedef struct _cl_kernel* cl_kernel;

namespace NES::Runtime::Execution {

class OpenCLPipelineStage : public ExecutablePipelineStage {
  public:
    OpenCLPipelineStage(const std::string& openCLKernelCode, size_t deviceIdIndex, uint64_t inputRecordSize, uint64_t outputRecordSize);
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
    execute(TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext, WorkerContext& workerContext);

    /**
     * @brief Must be called exactly once per executable pipeline to remove operator state.
     * @param pipelineExecutionContext
     * @return 0 if no error occurred.
     */
    virtual uint32_t stop(PipelineExecutionContext& pipelineExecutionContext);
    ~OpenCLPipelineStage() = default;

  private:
    const std::string openCLKernelCode;
    const size_t deviceIdIndex;
    const uint64_t inputRecordSize;
    const uint64_t outputRecordSize;
    cl_device_id deviceId;
    cl_context context;
    cl_program program;
    cl_kernel kernel;
    cl_command_queue commandQueue;
};

}// namespace NES::Runtime::Execution

namespace NES::QueryCompilation {

class OpenCLPipelineStage : public ExecutableOperator {};

}// namespace NES::QueryCompilation

#endif//NES_EXECUTABLEOPENCLOPERATOR_H
