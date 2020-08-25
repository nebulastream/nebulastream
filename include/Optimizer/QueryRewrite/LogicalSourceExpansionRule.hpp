#ifndef NES_LOGICALSOURCEEXPANSIONRULE_HPP
#define NES_LOGICALSOURCEEXPANSIONRULE_HPP

#include <memory>

namespace NES {

class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class LogicalSourceExpansionRule;
typedef std::shared_ptr<LogicalSourceExpansionRule> LogicalSourceExpansionRulePtr;

class LogicalSourceExpansionRule {
  public:
    static LogicalSourceExpansionRulePtr create(StreamCatalogPtr);
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit LogicalSourceExpansionRule(StreamCatalogPtr);
    StreamCatalogPtr streamCatalog;
    std::tuple<OperatorNodePtr, std::vector<OperatorNodePtr>> getLogicalGraphToDuplicate(OperatorNodePtr operatorNode);
};
}// namespace NES
#endif//NES_LOGICALSOURCEEXPANSIONRULE_HPP
