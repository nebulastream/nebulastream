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
#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_

#include <QueryCompiler/Operators/PhysicalOperators/PhysicalUnaryOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace PhysicalOperators {

/**
 * @brief Physical watermark assignment operator.
 */
class PhysicalWatermarkAssignmentOperator : public PhysicalUnaryOperator {
  public:
    PhysicalWatermarkAssignmentOperator(OperatorId id,
                                        SchemaPtr inputSchema,
                                        SchemaPtr outputSchema,
                                        Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    static PhysicalOperatorPtr create(OperatorId id,
                                      const SchemaPtr& inputSchema,
                                      const SchemaPtr& outputSchema,
                                      Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor);
    static PhysicalOperatorPtr
    create(SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor);
    std::string toString() const override;
    OperatorNodePtr copy() override;

    /**
    * @brief Returns the watermark strategy.
    * @return  Windowing::WatermarkStrategyDescriptorPtr
    */
    Windowing::WatermarkStrategyDescriptorPtr getWatermarkStrategyDescriptor() const;

  private:
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};
}// namespace PhysicalOperators
}// namespace QueryCompilation
}// namespace NES

#endif  // NES_INCLUDE_QUERYCOMPILER_OPERATORS_PHYSICALOPERATORS_PHYSICALWATERMARKASSIGNMENTOPERATOR_HPP_
