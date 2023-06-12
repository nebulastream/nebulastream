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
#ifndef NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_KEYEDTIMEWINDOW_GENERATABLEKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
#define NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_KEYEDTIMEWINDOW_GENERATABLEKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

class GeneratableKeyedThreadLocalPreAggregationOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable slice merging operator, which consumes slices and merges them in the operator state.
     * @param inputSchema of the input records
     * @param outputSchema of the result records
     * @param operatorHandler handler of the operator state
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
           std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);

    /**
     * @brief Creates a new generatable slice merging operator, which consumes slices and merges them in the operator state.
     * @param id operator id
     * @param inputSchema of the input records
     * @param outputSchema of the result records
     * @param operatorHandler handler of the operator state
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(OperatorId id,
           SchemaPtr inputSchema,
           SchemaPtr outputSchema,
           Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
           std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);

    /**
    * @brief Code generation function for the execute call of an operator.
    * The execute function is called for each tuple buffer consumed by this operator.
    * @param codegen reference to the code generator.
    * @param context reference to the current pipeline context.
    */
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
     * @brief Code generation function for the open call of an operator.
     * The open function is called once per operator to initialize local state by a single thread.
     * @param codegen reference to the code generator.
     * @param context reference to the current pipeline context.
     */
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    [[nodiscard]] std::string toString() const override;

    OperatorNodePtr copy() override;

  private:
    GeneratableKeyedThreadLocalPreAggregationOperator(
        OperatorId id,
        SchemaPtr inputSchema,
        SchemaPtr outputSchema,
        Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandlerPtr operatorHandler,
        std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);

    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation;
    Windowing::Experimental::KeyedThreadLocalPreAggregationOperatorHandlerPtr windowHandler;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_CORE_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_KEYEDTIMEWINDOW_GENERATABLEKEYEDTHREADLOCALPREAGGREGATIONOPERATOR_HPP_
