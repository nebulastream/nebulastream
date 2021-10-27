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
#ifndef NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_WATERMARK_ASSIGNMENT_OPERATOR_HPP_
#define NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_WATERMARK_ASSIGNMENT_OPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES::QueryCompilation::GeneratableOperators {

/**
 * @brief Generates the watermark assignment operator.
 * Determines the watermark ts according to a WatermarkStrategyDescriptor an places it in the current buffer.
 */
class GeneratableWatermarkAssignmentOperator : public GeneratableOperator {
  public:
    /**
     * @brief Creates a new generatable watermark assignment operator, which computes a watermark ts an set it to the current buffer.
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param watermarkStrategyDescriptor the watermark strategy
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    /**
     * @brief Creates a new generatable watermark assignment operator, which computes a watermark ts an set it to the current buffer.
     * @param id operator id
     * @param inputSchema the input schema
     * @param outputSchema the output schema
     * @param watermarkStrategyDescriptor the watermark strategy
     * @return GeneratableOperatorPtr
     */
    static GeneratableOperatorPtr create(OperatorId id,
                                         SchemaPtr inputSchema,
                                         SchemaPtr outputSchema,
                                         Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    [[nodiscard]] std::string toString() const override;
    OperatorNodePtr copy() override;
    ~GeneratableWatermarkAssignmentOperator() noexcept override = default;

  private:
    GeneratableWatermarkAssignmentOperator(OperatorId id,
                                           SchemaPtr inputSchema,
                                           SchemaPtr outputSchema,
                                           Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};
}// namespace NES::QueryCompilation::GeneratableOperators
#endif  // NES_INCLUDE_QUERY_COMPILER_OPERATORS_GENERATABLE_OPERATORS_GENERATABLE_WATERMARK_ASSIGNMENT_OPERATOR_HPP_
