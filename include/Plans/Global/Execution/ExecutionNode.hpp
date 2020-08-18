#ifndef NES_EXECUTIONNODE_H
#define NES_EXECUTIONNODE_H

#include <API/QueryId.hpp>
#include <Nodes/Node.hpp>
#include <list>
#include <map>
#include <memory>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class NESTopologyEntry;
typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

/**
 * This class contains information about the physical node, a map of query sub plans that need to be executed
 * on the physical node, and some additional configurations.
 */
class ExecutionNode : public Node {

  public:
    static ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode, QueryId queryId, OperatorNodePtr operatorNode);
    static ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode);

    ~ExecutionNode() = default;

    /**
     * Check if a query sub plan with given Id exists or not
     * @param queryId : Id of the sub plan
     * @return true if the plan exists else false
     */
    bool hasQuerySubPlan(QueryId queryId);

    /**
     * Check if the query sub plan consists of operator node or not.
     * @param queryId : the plan Id
     * @param operatorNode : operator node
     * @return true if the operator exists else false
     */
    bool checkIfQuerySubPlanContainsOperator(QueryId queryId, OperatorNodePtr operatorNode);

    /**
     * Get Query subPlan for the given Id
     * @param queryId
     * @return Query sub plan
     */
    QueryPlanPtr getQuerySubPlan(QueryId queryId);

    /**
     * Remove existing subPlan
     * @param queryId
     * @return true if operation succeeds
     */
    bool removeQuerySubPlan(QueryId queryId);

    /**
     * add new query sub plan to the execution node
     * @param queryId: id of the sub plan
     * @param querySubPlan: query sub plan graph
     * @return true if operation succeeds
     */
    bool createNewQuerySubPlan(QueryId queryId, OperatorNodePtr operatorNode);

    /**
     * Append the Operators to the query sub plan
     * @param queryId: id of the sub plan
     * @param querySubPlan: query sub plan graph
     * @return true if operation succeeds
     */
    bool appendOperatorToQuerySubPlan(QueryId queryId, OperatorNodePtr operatorNode);

    /**
     * Get execution node id
     * @return id of the execution node
     */
    uint64_t getId();

    /**
     * Get the nes node for the execution node.
     * @return the nes node
     */
    NESTopologyEntryPtr getNesNode();

    /**
     * Create a new entry for query sub plan
     * @param queryId : the query ID
     * @param querySubPlan : the query sub plan
     * @return true if operation is successful
     */
    bool createNewQuerySubPlan(QueryId queryId, QueryPlanPtr querySubPlan);

    /**
     * Update an existing query sub plan
     * @param queryId : query id
     * @param querySubPlan : the new query sub plan
     * @return true if successful
     */
    bool updateQuerySubPlan(QueryId queryId, QueryPlanPtr querySubPlan);

    /**
     * Get the map of all query sub plans
     * @return
     */
    std::map<uint64_t, QueryPlanPtr> getAllQuerySubPlans();

    bool equal(NodePtr rhs) const override;

    const std::string toString() const override;

  private:
    explicit ExecutionNode(NESTopologyEntryPtr nesNode, QueryId queryId, OperatorNodePtr operatorNode);

    explicit ExecutionNode(NESTopologyEntryPtr nesNode);

    /**
     * Execution node id.
     * Same as physical node id.
     */
    const uint64_t id;

    /**
     * Physical Node information
     */
    const NESTopologyEntryPtr nesNode;

    /**
     * map of queryPlans
     */
    std::map<uint64_t, QueryPlanPtr> mapOfQuerySubPlans;

    /**
     * Free the resources occupied by the query sub plan.
     * @param querySubPlan : the input query sub plan
     */
    void freeOccupiedResources(QueryPlanPtr querySubPlan);
};
}// namespace NES

#endif//NES_EXECUTIONNODE_H
