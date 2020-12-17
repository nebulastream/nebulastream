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

#ifndef NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP
#define NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP

#include <Operators/LogicalOperators/Arity/UnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/LogicalOperatorForwardRefs.hpp>
#include <Windowing/WindowingForwardRefs.hpp>

namespace NES {

class WatermarkAssignerLogicalOperatorNode : public UnaryOperatorNode {
  public:
    WatermarkAssignerLogicalOperatorNode(const Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor,
                                         OperatorId id);

    Windowing::WatermarkStrategyDescriptorPtr getWatermarkStrategyDescriptor() const;

    bool equal(const NodePtr rhs) const override;

    bool isIdentical(NodePtr rhs) const override;

    const std::string toString() const override;

    OperatorNodePtr copy() override;

  private:
    Windowing::WatermarkStrategyDescriptorPtr watermarkStrategyDescriptor;
};

typedef std::shared_ptr<WatermarkAssignerLogicalOperatorNode> WatermarkAssignerLogicalOperatorNodePtr;

}// namespace NES

#endif//NES_WATERMARKASSIGNERLOGICALOPERATORNODE_HPP
