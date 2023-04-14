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
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalPythonUDFOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators::Experimental {

PhysicalPythonUDFOperator::PhysicalPythonUDFOperator(OperatorId id,
                                                     SchemaPtr inputSchema,
                                                     SchemaPtr outputSchema,
                                                     Runtime::Execution::ExecutablePipelineStagePtr executablePipelineStage)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)),
      executablePipelineStage(std::move(executablePipelineStage)) {}

PhysicalOperatorPtr
PhysicalPythonUDFOperator::create(const SchemaPtr& inputSchema,
                                  const SchemaPtr& outputSchema,
                                  const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, executablePipelineStage);
}
PhysicalOperatorPtr
PhysicalPythonUDFOperator::create(OperatorId id,
                                  const SchemaPtr& inputSchema,
                                  const SchemaPtr& outputSchema,
                                  const Runtime::Execution::ExecutablePipelineStagePtr& executablePipelineStage) {
    return std::make_shared<PhysicalPythonUDFOperator>(id, inputSchema, outputSchema, executablePipelineStage);
}

std::string PhysicalPythonUDFOperator::toString() const { return "PhysicalPythonUDFOperator"; }

OperatorNodePtr PhysicalPythonUDFOperator::copy() { return create(id, inputSchema, outputSchema, executablePipelineStage); }

Runtime::Execution::ExecutablePipelineStagePtr PhysicalPythonUDFOperator::getExecutablePipelineStage() {
    return executablePipelineStage;
}
}// namespace NES::QueryCompilation::PhysicalOperators::Experimental
#endif
