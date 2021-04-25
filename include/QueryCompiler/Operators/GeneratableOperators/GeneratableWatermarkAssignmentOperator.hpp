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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableWatermarkAssignmentOperator : public GeneratableOperator {
  public:
    void generateExecute(CodeGeneratorPtr codegen, PipelineContextPtr context) override;
    const std::string toString() const override;

    static GeneratableOperatorPtr create(SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    static GeneratableOperatorPtr create(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                         Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    OperatorNodePtr copy() override;

  private:
    GeneratableWatermarkAssignmentOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema,
                                           Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEWATERMARKASSIGNMENTOPERATOR_HPP_
