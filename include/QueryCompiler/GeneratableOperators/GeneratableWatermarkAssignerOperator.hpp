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

#ifndef NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP
#define NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP

#include <Operators/LogicalOperators/WatermarkAssignerLogicalOperatorNode.hpp>
#include <QueryCompiler/GeneratableOperators/GeneratableOperator.hpp>
#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>

namespace NES {

class GeneratableWatermarkAssignerOperator : public WatermarkAssignerLogicalOperatorNode, public GeneratableOperator {
  public:
    static GeneratableWatermarkAssignerOperatorPtr
    create(WatermarkAssignerLogicalOperatorNodePtr watermarkAssignerLogicalOperatorNode,
           OperatorId id = UtilityFunctions::getNextOperatorId());

    /**
   * @brief Produce function, which calls the child produce function and brakes pipelines if necessary.
   * @param codegen a pointer to the code generator.
   * @param context a pointer to the current pipeline context.
   */
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief Consume function, which generates code for the processing and calls the parent consume function.
    * @param codegen a pointer to the code generator.
    * @param context a pointer to the current pipeline context.
    */
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context) override;

    /**
    * @brief To string method for the operator.
    * @return string
    */
    const std::string toString() const override;

  private:
    GeneratableWatermarkAssignerOperator(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor,
                                         OperatorId id);
};
}// namespace NES

#endif//NES_GENERATABLEWATERMARKASSIGNEROPERATOR_HPP
