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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_SLICE_MERGING_OPERATOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_SLICE_MERGING_OPERATOR_HPP_
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

/**
 * @brief this class represents the intermediate merger for distributed windowing that is deployed on the worker
 * nodes between the slicer and combiner to do intermediate merging
 */
class SliceMergingOperator : public WindowOperatorNode {
  public:
    SliceMergingOperator(Windowing::LogicalWindowDefinitionPtr const& windowDefinition, OperatorId id);

    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    OperatorNodePtr copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;
};

}// namespace NES
#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_SLICE_MERGING_OPERATOR_HPP_
