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

#ifndef NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_LOGICAL_SOURCE_EXPANSION_RULE_HPP_
#define NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_LOGICAL_SOURCE_EXPANSION_RULE_HPP_

#include <Optimizer/QueryRewrite/BaseRewriteRule.hpp>
#include <memory>
#include <set>

namespace NES {

class StreamCatalog;
using StreamCatalogPtr = std::shared_ptr<StreamCatalog>;

class QueryPlan;
using QueryPlanPtr = std::shared_ptr<QueryPlan>;

class OperatorNode;
using OperatorNodePtr = std::shared_ptr<OperatorNode>;
}// namespace NES

namespace NES::Optimizer {
class LogicalSourceExpansionRule;
using LogicalSourceExpansionRulePtr = std::shared_ptr<LogicalSourceExpansionRule>;

/**
 * @brief This class will expand the logical query graph by adding information about the physical sources and expand the
 * graph by adding additional replicated operators.
 *
 * Note: Apart from replicating the logical source operator we also replicate its down stream operators till we
 * encounter first n-ary operator or we encounter sink operator.
 *
 * Example: a query :                       Sink
 *                                           |
 *                                           Map
 *                                           |
 *                                        Source(Car)
 *
 * will be expanded to:                            Sink
 *                                                /     \
 *                                              /        \
 *                                           Map1        Map2
 *                                           |             |
 *                                      Source(Car1)    Source(Car2)
 *
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2
 *
 *                                                     or
 *
 * a query :                                    Sink
 *                                               |
 *                                             Merge
 *                                             /   \
 *                                            /    Filter
 *                                          /        |
 *                                   Source(Car)   Source(Truck)
 *
 * will be expanded to:                             Sink
 *                                                   |
 *                                                 Merge
 *                                              /  / \   \
 *                                            /  /    \    \
 *                                         /   /       \     \
 *                                       /   /          \      \
 *                                    /    /         Filter1   Filter2
 *                                 /     /               \         \
 *                               /      /                 \          \
 *                         Src(Car1) Src(Car2)       Src(Truck1)  Src(Truck2)
 *
 * Given that logical source car has two physical sources: i.e. car1 and car2 and
 * logical source truck has two physical sources: i.e. truck1 and truck2
 *
 */
class LogicalSourceExpansionRule : public BaseRewriteRule {
  public:
    static LogicalSourceExpansionRulePtr create(StreamCatalogPtr);

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan) override;

    virtual ~LogicalSourceExpansionRule() = default;

  private:
    explicit LogicalSourceExpansionRule(StreamCatalogPtr);
    StreamCatalogPtr streamCatalog;

    /**
     * @brief Returns the duplicated operator and its chain of upstream operators till first n-ary or sink operator is found
     * @param operatorNode : start operator
     * @return a tuple of duplicated operator with its duplicated downstream operator chains and a vector of original head operators
     */
    std::tuple<OperatorNodePtr, std::set<OperatorNodePtr>> getLogicalGraphToDuplicate(const OperatorNodePtr& operatorNode);
};
}// namespace NES::Optimizer
#endif// NES_INCLUDE_OPTIMIZER_QUERY_REWRITE_LOGICAL_SOURCE_EXPANSION_RULE_HPP_
