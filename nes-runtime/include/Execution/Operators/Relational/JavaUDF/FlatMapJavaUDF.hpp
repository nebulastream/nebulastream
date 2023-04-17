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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FLATMAPJAVAUDF_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FLATMAPJAVAUDF_HPP_

#ifdef ENABLE_JNI

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <jni.h>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This operator evaluates a flat map expression defined as java function on input records.
 * Its state is managed inside a JavaUDFOperatorHandler.
 */
class FlatMapJavaUDF : public ExecutableOperator {
  public:
    /**
     * @brief Creates a FlatMapJavaUDF operator
     * @param operatorHandlerIndex The index to a valid JavaUDFOperatorHandler
     * @param inputSchema The input schema of the flat map tuples. Same as in the handler.
     * @param outputSchema The output schema of the flat map tuples Same as in the handler.
     */
    FlatMapJavaUDF(uint64_t operatorHandlerIndex, SchemaPtr inputSchema, SchemaPtr outputSchema)
        : operatorHandlerIndex(operatorHandlerIndex), inputSchema(inputSchema), outputSchema(outputSchema){};

    void* executeFlatMapUDF(void* state, void* pojoObjectPtr);

    /**
     * Operator execution function
     * @param ctx operator context
     * @param record input record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * Terminate operator
     * @param ctx execution context
     */
    void terminate(ExecutionContext& ctx) const override;

  private:
    const uint64_t operatorHandlerIndex;

    // These needs to be the same Schemas as used in the operator handler.
    // We need them here to support some functionality during for-loops in execute where we cannot access the handler.
    const SchemaPtr inputSchema, outputSchema;
};

}// namespace NES::Runtime::Execution::Operators

#endif//ENABLE_JNI
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FLATMAPJAVAUDF_HPP_
