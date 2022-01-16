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

#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_MAX_AGGREGATION_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_MAX_AGGREGATION_HPP_
#include <QueryCompiler/Operators/GeneratableOperators/Windowing/Aggregations/GeneratableWindowAggregation.hpp>
#include <QueryCompiler/PipelineContext.hpp>
namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

class GeneratableMaxAggregation : public GeneratableWindowAggregation {
  public:
    explicit GeneratableMaxAggregation(Windowing::WindowAggregationDescriptorPtr aggregationDescriptor);
    ~GeneratableMaxAggregation() noexcept override = default;
    /**
     * @brief Factory Method to create a new GeneratableWindowAggregation
     * @param aggregationDescriptor Window aggregation descriptor
     * @return GeneratableWindowAggregationPtr
     */
    static GeneratableWindowAggregationPtr create(const Windowing::WindowAggregationDescriptorPtr& aggregationDescriptor);
    /**
     * @brief Generates code for window aggregate
     * @param currentCode current code pointer
     * @param partialValueRef partial value ref
     * @param inputStruct input struct
     * @param inputRef input value reference
     */
    void compileLiftCombine(CompoundStatementPtr currentCode,
                            BinaryOperatorStatement partialRef,
                            RecordHandlerPtr recordHandler) override;

    void generateHeaders(PipelineContextPtr context) override;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif// NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_WINDOWING_AGGREGATIONS_GENERATABLE_MAX_AGGREGATION_HPP_
