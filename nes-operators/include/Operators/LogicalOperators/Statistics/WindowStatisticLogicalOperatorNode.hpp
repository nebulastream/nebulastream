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

#ifndef NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSLOGICALOPERATORNODE_HPP_
#define NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSLOGICALOPERATORNODE_HPP_

#include <Operators/LogicalOperators/LogicalUnaryOperatorNode.hpp>
#include <Operators/LogicalOperators/Statistics/StatisticCollectorType.hpp>

namespace NES::Experimental::Statistics {

class WindowStatisticDescriptor;
using WindowStatisticDescriptorPtr = std::shared_ptr<WindowStatisticDescriptor>;

class WindowStatisticLogicalOperatorNode : public LogicalUnaryOperatorNode {
  public:
    WindowStatisticLogicalOperatorNode(WindowStatisticDescriptorPtr statisticDescriptor,
                                       const std::string& logicalSourceName,
                                       const std::string& physicalSourceName,
                                       const std::string& synopsisSourceDataFieldName,
                                       TopologyNodeId topologyNodeId,
                                       StatisticCollectorType statisticCollectorType,
                                       time_t windowSize,
                                       time_t slideFactor,
                                       OperatorId id);

    std::string getTypeAsString() const;

    std::string toString() const override;

    OperatorNodePtr copy() override;

    [[nodiscard]] bool equal(NodePtr const& rhs) const override;

    [[nodiscard]] bool isIdentical(NodePtr const& rhs) const override;

    bool inferSchema() override;

    void inferStringSignature() override;

    WindowStatisticDescriptorPtr getSynopsisDescriptor() const;

    const std::string& getLogicalSourceName() const;

    const std::string& getPhysicalSourceName() const;

    const std::string& getSynopsisSourceDataFieldName() const;

    StatisticCollectorType getStatisticCollectorType() const;

    const WindowStatisticDescriptorPtr& getStatisticDescriptor() const;

    TopologyNodeId getTopologyNodeId() const;

    time_t getWindowSize() const;

    time_t getSlideFactor() const;

  private:
    std::string logicalSourceName;
    std::string physicalSourceName;
    std::string synopsisSourceDataFieldName;
    TopologyNodeId topologyNodeId;
    StatisticCollectorType statisticCollectorType;
    WindowStatisticDescriptorPtr statisticDescriptor;
    time_t windowSize;
    time_t slideFactor;
};
}// namespace NES::Experimental::Statistics

#endif//NES_NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_STATISTIC_WINDOWSTATISTICSLOGICALOPERATORNODE_HPP_
