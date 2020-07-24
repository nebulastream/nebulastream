#ifndef NES_GLOBALQUERYPLAN_HPP
#define NES_GLOBALQUERYPLAN_HPP

#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GlobalQueryPlan;
typedef std::shared_ptr<GlobalQueryPlan> GlobalQueryPlanPtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * @brief This class is responsible for storing all currently running and to be deployed QueryPlans in the NES system.
 * The QueryPlans included in the GlobalQueryPlan can be fused together and therefore each operator in GQP contains
 * information about the set of queries it belongs to. The QueryPlans are bound together by a dummy logical root operator.
 */
class GlobalQueryPlan {
  public:
    static GlobalQueryPlanPtr create();

    /**
     * @brief Add operator of a query to the global queryPlan
     * @param queryId: Id of the query
     * @param operatorNode: logical operator to be added
     */
    void addOperator(std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief remove the operators belonging to the query with input query Id from the global query plan
     * @param queryId: the id of the query whose operators need to be removed
     */
    void removeQuery(std::string queryId);

    /**
     * @brief Get all sink operators in the query plan
     * @return vector of sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getAllSinkOperators();

    /**
     * @brief Get all source operators in the query plan
     * @return vector of source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getAllSourceOperators();

    /**
     * @brief Get all sink operators that are newly added to the global query plan
     * @return vector of newly added sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getNewSinkOperators();

    /**
     * @brief Get all source operators that are newly added to the global query plan
     * @return vector of newly added source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getNewSourceOperators();


  private:
    GlobalQueryPlan();
    OperatorNodePtr root;
    std::map<std::string, std::vector<OperatorNodePtr>> queryIdToOperatorMap;
    std::map<OperatorNodePtr, std::vector<std::string>> operatorToQueryIdMap;
};
}// namespace NES
#endif//NES_GLOBALQUERYPLAN_HPP
