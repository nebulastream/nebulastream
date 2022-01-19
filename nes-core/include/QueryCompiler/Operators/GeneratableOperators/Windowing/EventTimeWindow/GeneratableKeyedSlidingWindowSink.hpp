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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLEKEYEDSLIDINGWINDOWSINK_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLEKEYEDSLIDINGWINDOWSINK_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>
#include <Windowing/Experimental/TimeBasedWindow/KeyedEventTimeWindowHandler.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

class GeneratableKeyedSlidingWindowSink : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable slice merging operator, which consumes slices and merges them in the operator state.
     * @param inputSchema of the input records
     * @param outputSchema of the result records
     * @param operatorHandler handler of the operator state
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
                                         std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);

    /**
     * @brief Creates a new generatable slice merging operator, which consumes slices and merges them in the operator state.
     * @param id operator id
     * @param inputSchema of the input records
     * @param outputSchema of the result records
     * @param operatorHandler handler of the operator state
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
                                         std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    void generateOpen(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;

  private:
    GeneratableKeyedSlidingWindowSink(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Windowing::Experimental::KeyedEventTimeWindowHandlerPtr operatorHandler,
                                      std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation);
    std::vector<GeneratableOperators::GeneratableWindowAggregationPtr> windowAggregation;
    Windowing::Experimental::KeyedEventTimeWindowHandlerPtr windowHandler;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_GENERATABLEKEYEDSLIDINGWINDOWSINK_HPP_
