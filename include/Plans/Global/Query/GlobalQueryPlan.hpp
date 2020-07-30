#ifndef NES_GLOBALQUERYPLAN_HPP
#define NES_GLOBALQUERYPLAN_HPP

#include <iostream>
#include <map>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class GlobalQueryNode;
typedef std::shared_ptr<GlobalQueryNode> GlobalQueryNodePtr;

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
     * @brief Add query plan to the global query plan
     * @param queryPlan : new query plan to be added.
     */
    void addQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief remove the operators belonging to the query with input query Id from the global query plan
     * @param queryId: the id of the query whose operators need to be removed
     */
    void removeQuery(std::string queryId);

    /**
     * @brief Get all newly added Global Query Nodes with NodeType operators
     * @tparam NodeType: type of logical operator
     * @return vector of global query nodes
     */
    template<class NodeType>
    std::vector<GlobalQueryNodePtr> getAllNewGlobalQueryNodesWithOperatorType();

    /**
     * @brief Get all Global Query Nodes with NodeType operators
     * @tparam NodeType: type of logical operator
     * @return vector of global query nodes
     */
    template<class NodeType>
    std::vector<GlobalQueryNodePtr> getAllGlobalQueryNodesWithOperatorType();

  private:
    GlobalQueryPlan();

    /**
     * @brief Get next free node id
     * @return next free id
     */
    uint64_t getNextFreeId();

    /**
     * @brief Add a new logical operator of a query as child to a given GlobalQueryNode.
     * @param parentNode: the global query node to which a new global query node is to be added as child.
     * @param queryId: the query id to which the logical operator belongs
     * @param operatorNode: the logical operator
     */
    void addNewGlobalOperatorNodeAsChild(GlobalQueryNodePtr parentNode, std::string queryId, OperatorNodePtr operatorNode);

    /**
     * @brief Get all global query nodes containing given queryId
     * @param queryId: queryId for which to get global query nodes
     * @return vector containing all GlobalQueryNodes or empty list
     */
    std::vector<GlobalQueryNodePtr> getGlobalQueryNodesForQuery(std::string queryId);

    uint64_t freeGlobalQueryNodeId;
    GlobalQueryNodePtr root;
    std::map<std::string, std::vector<GlobalQueryNodePtr>> queryToGlobalQueryNodeMap;
    std::map<GlobalQueryNodePtr, std::vector<std::string>> globalQueryNodeToQueryMap;
};
}// namespace NES
#endif//NES_GLOBALQUERYPLAN_HPP
