#ifndef NES_EXECUTIONNODE_H
#define NES_EXECUTIONNODE_H

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
    static ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode, std::string subPlanId, OperatorNodePtr operatorNode);
    static ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode);

    ~ExecutionNode() = default;

    /**
     * Check if a query sub plan with given Id exists or not
     * @param subPlanId : Id of the sub plan
     * @return true if the plan exists else false
     */
    bool hasQuerySubPlan(std::string subPlanId);

    /**
     * Check if the query sub plan consists of operator node or not.
     * @param subPlanId : the plan Id
     * @param operatorNode : operator node
     * @return true if the operator exists else false
     */
    bool querySubPlanContainsOperator(std::string subPlanId, OperatorNodePtr operatorNode);

    /**
     * Get Query subPlan for the given Id
     * @param subPlanId
     * @return Query sub plan
     */
    QueryPlanPtr getQuerySubPlan(std::string subPlanId);

    /**
     * Remove existing subPlan
     * @param subPlanId
     * @return true if operation succeeds
     */
    bool removeQuerySubPlan(std::string subPlanId);

    /**
     * add new query sub plan to the execution node
     * @param subPlanId: id of the sub plan
     * @param querySubPlan: query sub plan graph
     * @return true if operation succeeds
     */
    bool createNewQuerySubPlan(std::string subPlanId, OperatorNodePtr operatorNode);

    /**
     * Append the Operators to the query sub plan
     * @param subPlanId: id of the sub plan
     * @param querySubPlan: query sub plan graph
     * @return true if operation succeeds
     */
    bool appendOperatorToQuerySubPlan(std::string subPlanId, OperatorNodePtr operatorNode);

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
     * @param subPlanId : the query ID
     * @param querySubPlan : the query sub plan
     * @return true if operation is successful
     */
    bool createNewQuerySubPlan(std::string subPlanId, QueryPlanPtr querySubPlan);

    /**
     * Update an existing query sub plan
     * @param subPlanId : query id
     * @param querySubPlan : the new query sub plan
     * @return true if successful
     */
    bool updateQuerySubPlan(std::string subPlanId, QueryPlanPtr querySubPlan);

    /**
     * Get the map of all query sub plans
     * @return
     */
    std::map<std::string, QueryPlanPtr> getAllQuerySubPlans();

    const std::string toString() const override;

  private:
    explicit ExecutionNode(NESTopologyEntryPtr nesNode, std::string subPlanId, OperatorNodePtr operatorNode);

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
    std::map<std::string, QueryPlanPtr> mapOfQuerySubPlans;

    /**
     * Free the resources occupied by the query sub plan.
     * @param querySubPlan : the input query sub plan
     */
    void freeOccupiedResources(QueryPlanPtr querySubPlan);
};
}// namespace NES

#endif//NES_EXECUTIONNODE_H
