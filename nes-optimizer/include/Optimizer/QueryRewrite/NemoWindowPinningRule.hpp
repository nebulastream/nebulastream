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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOWINDOWPINNINGRULE_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOWINDOWPINNINGRULE_HPP_

#include <Configurations/Coordinator/OptimizerConfiguration.hpp>
#include <Optimizer/QueryRewrite/DistributedWindowRule.hpp>
#include <unordered_map>

namespace NES {
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class WindowOperatorNode;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperatorNode>;

class WatermarkAssignerLogicalOperatorNode;
using WatermarkAssignerLogicalOperatorNodePtr = std::shared_ptr<WatermarkAssignerLogicalOperatorNode>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

}// namespace NES

namespace NES::Optimizer {

class NemoWindowPinningRule;
using NemoWindowPinningRulePtr = std::shared_ptr<NemoWindowPinningRule>;

/**
 * @brief This rule will replace the logical window operator with either a centralized or distributed implementation.
 * The following rule applies:
 *      - if the logical window operators has more than one child, we will replace it with a distributed implementation, otherwise with a centralized
 *
 * Example: a query for centralized window:
 *                                          Sink
 *                                           |
 *                                           LogicalWindow
 *                                           |
 *                                        Source(Car)
     will be changed to:
 *
 *                                                  Sink
 *                                                  |
 *                                              LogicalWindow
 *                                                   |
 *                                        Source(Car)    Source(Car)
 *
 * Example: a query for distributed window:
 *
 *
 *
 *                                                 Sink
 *                                                     |
 *                                              WindowCombiner
 *                                                /     \
 *                                              /        \
 *                                    WindowSlicer        WindowSlicer
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 * ---------------------------------------------
 * Example: a query :                       Sink
 *                                           |
 *                                           Window
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                        Sink
 *                                               |
 *                                          Window-Combiner
 *                                                |
 *                                          Watermark-Assigner
 *                                             /    \
 *                                           /       \
 *                            Window-SliceCreator Window-SliceCreator
 *                                      |               |
 *                             Watermark-Assigner   Watermark-Assigner
 *                                      |               |
 *                                   Source(Car1)    Source(Car2)
*/
class NemoWindowPinningRule : public DistributedWindowRule {
  public:
    /**
     * @brief Creates a new NemoWindowPinningRule with specific thresholds that influence when the rule is applied.
     * @param windowDistributionChildrenThreshold The number of child nodes from which on we will replace a central window operator with a distributed window operator.
     * @param windowDistributionCombinerThreshold The number of child nodes from which on we will introduce combiner
     * @return NemoWindowPinningRulePtr
     */
    static NemoWindowPinningRulePtr create(Configurations::OptimizerConfiguration configuration, TopologyPtr topologyNode);
    virtual ~NemoWindowPinningRule() = default;
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit NemoWindowPinningRule(Configurations::OptimizerConfiguration configuration, TopologyPtr topology);
    void createCentralWindowOperator(const WindowOperatorNodePtr& windowOp);

    /**
     * @brief This helper method identifies the nodes where the window operator should be placed its sources.
     * @param operatorNode the operator node
     * @param sharedParentThreshold threshold that identifies when to combine based on the number of sources of an operator
     * @return map with the node id where the window operator shall be placed and its sources
     */
    std::unordered_map<uint64_t, std::vector<WatermarkAssignerLogicalOperatorNodePtr>>
    getMergerNodes(OperatorNodePtr operatorNode, uint64_t sharedParentThreshold);

    /**
     * @brief The NEMO placement is identifying in a hierarchical tree shared parents and places there a CentralWindowOperator.
     * The conditions for placing the CentralWindowOperator are based on the distributedChildThreshold and distributed CombinerThreshold parameters.
     * @param windowOp the window operator node
     * @param queryPlan the query plan
     */
    void pinWindowOperators(const WindowOperatorNodePtr& windowOp, const QueryPlanPtr& queryPlan);

    /**
     * @deprecated Replaced by pinWindowOperators as it is using the old window operator implementation
     */
    void createDistributedWindowOperator(const WindowOperatorNodePtr& windowOp, const QueryPlanPtr& queryPlan);

    bool performDistributedWindowOptimization;
    // The number of child nodes from which on we will replace a central window operator with a distributed window operator.
    uint64_t windowDistributionChildrenThreshold;
    // The number of child nodes from which on we will introduce combiner
    uint64_t windowDistributionCombinerThreshold;
    TopologyPtr topology;
    bool enableNemoPlacement;
};
}// namespace NES::Optimizer
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_QUERYREWRITE_NEMOWINDOWPINNINGRULE_HPP_
