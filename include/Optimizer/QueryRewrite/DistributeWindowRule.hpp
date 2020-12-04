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

#ifndef NES_DistributeWindowRule_HPP
#define NES_DistributeWindowRule_HPP

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>

#include <memory>

namespace NES {
class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class DistributeWindowRule;
typedef std::shared_ptr<DistributeWindowRule> DistributeWindowRulePtr;

class WindowOperatorNode;
typedef std::shared_ptr<WindowOperatorNode> WindowOperatorNodePtr;

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

 */
class DistributeWindowRule : public BaseRefinementRule {
  public:
    // The number of child nodes from which on we will replace a central window operator with a distributed window operator.
    static const uint64_t CHILD_NODE_THRESHOLD = 2;

    // The number of child nodes from which on we will introduce combinerr
    static const uint64_t CHILD_NODE_THRESHOLD_COMBINER = 4;

    static DistributeWindowRulePtr create();

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit DistributeWindowRule();
    void createCentralWindowOperator(WindowOperatorNodePtr currentWindowOperator);
    void createDistributedWindowOperator(WindowOperatorNodePtr logicalWindowOperaotr);
};
}// namespace NES
#endif//NES_DistributeWindowRule_HPP
