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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPYTHONUDFOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPYTHONUDFOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/AbstractEmitOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/AbstractScanOperator.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

/**
 * @brief Physical python udf operator. It receives an executable pipeline stage, which captures specific user defined logic.
 */
class PhysicalPythonUdfOperator : public PhysicalOperators::PhysicalUnaryOperator,
                                  public PhysicalOperators::AbstractEmitOperator,
                                  public PhysicalOperators::AbstractScanOperator {
  public:
    /**
     * @brief Creates a new physical python udf operator, which contains a python udf executable pipeline stage
     * @param id operator id
     * @param inputSchema input schema
     * @param outputSchema output schema
     * @param executablePipelineStage executable pipeline stage
     */
    PhysicalPythonUdfOperator(OperatorId id,
                              SchemaPtr inputSchema,
                              SchemaPtr outputSchema,
                              Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage);
    static PhysicalOperators::PhysicalOperatorPtr
    create(OperatorId id,
           const SchemaPtr& inputSchema,
           const SchemaPtr& outputSchema,
           const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage);
    static PhysicalOperators::PhysicalOperatorPtr
    create(const SchemaPtr& inputSchema,
           const SchemaPtr& outputSchema,
           const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
    * @brief get executable pipeline stage
    * @return ExecutablePipelineStagePtr
    */
    Runtime::Execution::ExecutablePipelineStagePtr getExecutablePipelineStage();

  private:
    Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage;
};
}// namespace NES::QueryCompilation::PhysicalOperators::Experimental
#endif// NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPYTHONUDFOPERATOR_HPP_
#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALPYTHONUDFOPERATOR_HPP_
