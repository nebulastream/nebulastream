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

#ifndef NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEWINDOWRULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEWINDOWRULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <Configurations/Coordinator/OptimizerConfiguration.hpp>

namespace NES {
class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;

class WindowOperatorNode;
using WindowOperatorNodePtr = std::shared_ptr<WindowOperatorNode>;

}// namespace NES

namespace NES::Optimizer {

class DistributeWindowRule;
using DistributeWindowRulePtr = std::shared_ptr<DistributeWindowRule>;

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
class DistributeWindowRule : public BaseRewriteRule {
  public:
    static const uint64_t CHILD_NODE_THRESHOLD = 2;

    // The number of child nodes from which on we will introduce combinerr
    static const uint64_t CHILD_NODE_THRESHOLD_COMBINER = 4;
    /**
     * @brief Creates a new DistributeWindowRule with specific thresholds that influence when the rule is applied.
     * @param windowDistributionChildrenThreshold The number of child nodes from which on we will replace a central window operator with a distributed window operator.
     * @param windowDistributionCombinerThreshold The number of child nodes from which on we will introduce combiner
     * @return DistributeWindowRulePtr
     */
    static DistributeWindowRulePtr create(Configurations::OptimizerConfiguration configuration);
    virtual ~DistributeWindowRule() = default;
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

  private:
    explicit DistributeWindowRule(Configurations::OptimizerConfiguration configuration);
    void createCentralWindowOperator(const WindowOperatorNodePtr& windowOp);
    void createDistributedWindowOperator(const WindowOperatorNodePtr& logicalWindowOperator, const QueryPlanPtr& queryPlan);

    bool performDistributedWindowOptimization;
    // The number of child nodes from which on we will replace a central window operator with a distributed window operator.
    uint64_t windowDistributionChildrenThreshold;
    // The number of child nodes from which on we will introduce combiner
    uint64_t windowDistributionCombinerThreshold;

};
}// namespace NES::Optimizer
#endif  // NES_INCLUDE_OPTIMIZER_QUERYREWRITE_DISTRIBUTEWINDOWRULE_HPP_
