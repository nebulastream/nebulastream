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
class DistributeWindowRule : public BaseRewriteRule {
  public:
    static DistributeWindowRulePtr create();

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit DistributeWindowRule();
};
}// namespace NES
#endif//NES_DistributeWindowRule_HPP
