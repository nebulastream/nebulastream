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

#ifndef NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_COMPUTATION_OPERATOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_COMPUTATION_OPERATOR_HPP_
#include <Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windowing/WindowOperatorNode.hpp>

namespace NES {

/**
 * @brief this class represents the computation operator for distributed windowing that is deployed on the sink node and which merges all slices
 */
class WindowComputationOperator : public WindowOperatorNode {
  public:
    WindowComputationOperator(Windowing::LogicalWindowDefinitionPtr const& windowDefinition, OperatorId id);

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;
    [[nodiscard]] std::string toString() const override;

    OperatorNodePtr copy() override;
    bool inferSchema() override;
    void inferStringSignature() override;
    std::string getClassName() override;
};

}// namespace NES
#endif// NES_INCLUDE_OPERATORS_LOGICAL_OPERATORS_WINDOWING_WINDOW_COMPUTATION_OPERATOR_HPP_
