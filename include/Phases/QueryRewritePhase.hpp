#ifndef NES_QUERYREWRITEPHASE_HPP
#define NES_QUERYREWRITEPHASE_HPP

#include <memory>

namespace NES {
class QueryRewritePhase;
typedef std::shared_ptr<QueryRewritePhase> QueryRewritePhasePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class LogicalSourceExpansionRule;
typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

class FilterPushDownRule;
typedef std::shared_ptr<FilterPushDownRule> FilterPushDownRulePtr;

class DistributeWindowRule;
typedef std::shared_ptr<DistributeWindowRule> DistributeWindowRulePtr;

/**
 * @brief This phase is responsible for re-writing the query plan
 */
class QueryRewritePhase {
  public:
    static QueryRewritePhasePtr create(StreamCatalogPtr streamCatalog);

    /**
     * @brief Perform query plan re-write for the input query plan
     * @param queryPlan : the input query plan
     * @return updated query plan
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

    ~QueryRewritePhase();

  private:
    explicit QueryRewritePhase(StreamCatalogPtr streamCatalog);
    LogicalSourceExpansionRulePtr logicalSourceExpansionRule;
    FilterPushDownRulePtr filterPushDownRule;
    DistributeWindowRulePtr distributeWindowRule;
};
}// namespace NES
#endif//NES_QUERYREWRITEPHASE_HPP
