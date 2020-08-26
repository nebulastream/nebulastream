#ifndef NES_QUERYREWRITEPHASE_HPP
#define NES_QUERYREWRITEPHASE_HPP

#include <memory>

namespace NES {
class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class FilterPushDownRule;
typedef std::shared_ptr<FilterPushDownRule> FilterPushDownRulePtr;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create();

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

    ~QueryRewritePhase();
  private:
    explicit QueryRewritePhase();
    FilterPushDownRulePtr filterPushDownRule;
};
}// namespace NES
#endif//NES_QUERYREWRITEPHASE_HPP
