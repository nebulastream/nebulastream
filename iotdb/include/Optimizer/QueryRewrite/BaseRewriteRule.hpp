#ifndef NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
#define NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_

#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

enum RewriteRuleType {
    FILTER_PUSH_DOWN
};

class BaseRewriteRule {

  public:

    /**
     * @brief Apply the rule to the Query plan
     * @param queryPlanPtr : The original query plan
     * @return The updated query plan
     */
    virtual QueryPlanPtr apply(QueryPlanPtr queryPlanPtr) = 0;

    /**
     * @brief Get the type of rule applied
     * @return the rule type
     */
    virtual RewriteRuleType getType() = 0;

};
}

#endif //NES_IMPL_OPTIMIZER_QUERYREWRITE_BASERULE_HPP_
