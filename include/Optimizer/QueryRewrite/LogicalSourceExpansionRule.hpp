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

/**
 * @brief This class will expand the logical query graph by adding information about the physical sources and expand the
 * graph by adding additional replicated operators.
 *
 * Note: Apart from replicating the logical source operator we also replicate its down stream operators till we
 * encounter first n-ary operator or we encounter sink operator.
 *
 */
class LogicalSourceExpansionRule {
  public:
    static LogicalSourceExpansionRulePtr create(StreamCatalogPtr);

    /**
     * @brief Apply Logical source expansion rule on input query plan
     * @param queryPlan: the original non-expanded query plan
     * @return expanded logical query plan
     */
    QueryPlanPtr apply(QueryPlanPtr queryPlan);

  private:
    explicit LogicalSourceExpansionRule(StreamCatalogPtr);
    StreamCatalogPtr streamCatalog;

    /**
     * @brief Returns the duplicated operator and its chain of upstream operators till first n-ary or sink operator is found
     * @param operatorNode : start operator
     * @return a tuple of duplicated operator with its duplicated downstream operator chains and a vector of original head operators
     */
    std::tuple<OperatorNodePtr, std::vector<OperatorNodePtr>> getLogicalGraphToDuplicate(OperatorNodePtr operatorNode);
};
}// namespace NES
#endif//NES_LOGICALSOURCEEXPANSIONRULE_HPP
