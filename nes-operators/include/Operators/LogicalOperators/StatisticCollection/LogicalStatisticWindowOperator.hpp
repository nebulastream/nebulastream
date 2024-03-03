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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_LOGICALSTATISTICWINDOWOPERATOR_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_LOGICALSTATISTICWINDOWOPERATOR_HPP_

#include <Operators/LogicalOperators/StatisticCollection/WindowStatisticDescriptor.hpp>
#include <Operators/LogicalOperators/LogicalUnaryOperator.hpp>
#include <Operators/LogicalOperators/Windows/WindowingForwardRefs.hpp>

namespace NES::Statistic {

class LogicalStatisticWindowOperator : public LogicalUnaryOperator {
  public:
    LogicalStatisticWindowOperator(OperatorId id,
                                   Windowing::WindowTypePtr windowType,
                                   WindowStatisticDescriptorPtr windowStatisticDescriptor);

    /**
     * @brief Infers the schema for this StatisticWindowOperatorNode
     * @return Success
     */
    bool inferSchema() override;

    /**
     * @brief Compares for equality
     * @param rhs
     * @return True, if equal, otherwise false.
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
     * @brief Checks if the nodes are equal
     * @param rhs
     * @return True, if equal and have the same ID, otherwise false.
     */
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    /**
     * @brief Infers the string signature
     */
    void inferStringSignature() override;

    /**
     * @brief Creates a string representation
     * @return std::string
     */
    std::string toString() const override;

    /**
     * @brief Creates a copy of this operator with
     * @return
     */
    OperatorPtr copy() override;

    /**
     * @brief Getter for the window type
     * @return WindowTypePtr
     */
    Windowing::WindowTypePtr getWindowType() const;

    /**
     * @brief Getter for the windowStatisticDescriptor
     * @return WindowStatisticDescriptorPtr
     */
    WindowStatisticDescriptorPtr getWindowStatisticDescriptor() const;

  private:
    Windowing::WindowTypePtr windowType;
    std::vector<OriginId> inputOriginIds;
    WindowStatisticDescriptorPtr windowStatisticDescriptor;
};

}// namespace NES::Statistic

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICCOLLECTION_LOGICALSTATISTICWINDOWOPERATOR_HPP_
