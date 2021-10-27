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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WATERMARK_ASSIGNER_LOGICAL_OPERATOR_NODE_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WATERMARK_ASSIGNER_LOGICAL_OPERATOR_NODE_HPP_

#include <Operators/AbstractOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES {

/**
 * @brief Watermark assignment operator, creates a watermark timestamp per input buffer.
 */
class WatermarkAssignerLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    WatermarkAssignerLogicalOperatorNode(Windowing::WatermarkStrategyDescriptorPtr const& watermarkStrategyDescriptor,
                                         OperatorId id);
    /**
    * @brief Returns the watermark strategy.
    * @return  Windowing::WatermarkStrategyDescriptorPtr
    */
    Windowing::WatermarkStrategyDescriptorPtr getWatermarkStrategyDescriptor() const;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    [[nodiscard]] std::string toString() const override;

    OperatorNodePtr copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;

  private:
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};

using WatermarkAssignerLogicalOperatorNodePtr = std::shared_ptr<WatermarkAssignerLogicalOperatorNode>;

}// namespace NES

#endif  // NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WATERMARK_ASSIGNER_LOGICAL_OPERATOR_NODE_HPP_
