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
#ifdef PYTHON_UDF_ENABLED
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PYTHONUDFEXECUTABLEPIPELINESTAGE_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PYTHONUDFEXECUTABLEPIPELINESTAGE_HPP_

#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/ExecutionResult.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <utility>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

using Runtime::TupleBuffer;
using Runtime::WorkerContext;

/**
 * @brief this executable pipeline stage is responsible for initiating and executing Python user-defined functions
 * Currently, it is only capable of handling integer arguments for python functions. This will be extended soon.
 */
class PythonUdfExecutablePipelineStage : public Runtime::Execution::ExecutablePipelineStage {
  public:
    /**
     * @brief The constructor for a pipeline stage executing Python Udf
     * @param inputSchema input schema pointer
     */
    explicit PythonUdfExecutablePipelineStage(const SchemaPtr& inputSchema);
    ~PythonUdfExecutablePipelineStage() override;
    ExecutionResult
    execute(Runtime::TupleBuffer& buffer, Runtime::Execution::PipelineExecutionContext& ctx, Runtime::WorkerContext& wc) override;

  private:
    // TODO make configurable via config file ideally
    const char* udfDirectory = "test_data/"; // directory which contains the udf file
    const char* udfFilename = "PythonUdf";   // name of the udf file
    const char* pythonFunctionName = "add42";// function name that is called
    const char* pythonSystemPathKey = "path";// keyword to set the Py system path
    SchemaPtr inputSchema;
};

}// namespace NES::QueryCompilation::PhysicalOperators::Experimental
#endif// NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PYTHONUDFEXECUTABLEPIPELINESTAGE_HPP_
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PYTHONUDFEXECUTABLEPIPELINESTAGE_HPP_
