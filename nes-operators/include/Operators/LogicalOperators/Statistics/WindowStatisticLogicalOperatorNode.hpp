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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICLOGICALOPERATORNODE_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>

namespace NES::Experimental::Statistics {

class WindowStatisticDescriptor;
using WindowStatisticDescriptorPtr = std::shared_ptr<WindowStatisticDescriptor>;

/**
 * @brief the class that describes the windowStatisticLogicalOperatorNode. A general operator that implements statistical operations.
 */
class WindowStatisticLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    /**
     * @brief the constructor of the windowStatisticLogicalOperatorNode
     * @param statisticDescriptor a statistic descriptor that holds specific values to the statistic
     * @param id the id of the operator
     */
    WindowStatisticLogicalOperatorNode(WindowStatisticDescriptorPtr statisticDescriptor, OperatorId id);

    /**
     * @return returns a string to easily put the current state of the object into words and numbers
     */
    std::string toString() const override;

    /**
     * @return returns a copy of the operator
     */
    OperatorNodePtr copy() override;

    /**
     * @brief checks two nodes are equal
     * @param a second node to compare to
     * @return true, if the nodes are equal, otherwise false
     */
    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    /**
     * @brief checks if two nodes are truly identical, meaning they are equal and also have the same operator id
     * @param rhs the other node
     * @return true, if they are equal and also share the same OperatorId
     */
    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    /**
     * @brief sets the input and output schema of the operator
     * @return true if successful, otherwise false
     */
    bool inferSchema() override;

    void inferStringSignature() override;

    /**
     * @return returns the statisticDescriptor of the statistic operator node
     */
    const WindowStatisticDescriptorPtr& getStatisticDescriptor() const;

  private:
    WindowStatisticDescriptorPtr statisticDescriptor;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTICS_WINDOWSTATISTICLOGICALOPERATORNODE_HPP_
