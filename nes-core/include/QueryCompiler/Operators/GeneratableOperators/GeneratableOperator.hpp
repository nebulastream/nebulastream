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

#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_OPERATOR_HPP_

#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators.
 * It defines open, execute and close functions as a general code generation interface for all operators.
 */
class GeneratableOperator : public UnaryOperatorNode {

  public:
    /**
     * @brief Code generation function for the open call of an operator.
     * The open function is called once per operator to initialize local state by a single thread.
     * @param codegen reference to the code generator.
     * @param context reference to the current pipeline context.
     */
    virtual void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context);

    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    *  @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    virtual void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) = 0;

    /**
     * @brie Code generation for the close call of an operator.
     * @param codegen reference to the code generator.
     * @param context reference to the current pipeline context.
     */
    virtual void generateClose(CodeGeneratorPtr codegen, PipelineContextPtr context);

    /**
     * Adds headers required by GeneratableOperator to context
     * @param context reference to the current pipeline context.
     */
    virtual void generateHeaders(PipelineContextPtr context);

    ~GeneratableOperator() noexcept override = default;

  protected:
    GeneratableOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema);
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_OPERATOR_HPP_
