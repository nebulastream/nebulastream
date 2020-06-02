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

    explicit ExecutionNode(NESTopologyEntryPtr nesNode, uint64_t subPlanId, QueryPlanPtr querySubPlan);
    ~ExecutionNode() = default;

    /**
     * Remove existing subPlan
     * @param subPlanId
     * @return true if operation succeeds
     */
    bool removeSubPlan(uint64_t subPlanId);

    /**
     * add new query sub plan to the execution node
     * @param subPlanId: id of the sub plan
     * @param querySubPlan: query sub plan graph
     * @return true if operation succeeds
     */
    bool addNewSubPlan(uint64_t subPlanId, QueryPlanPtr querySubPlan);

    /**
     * append the logical operator to the query sub plan
     * @param subPlanId: id of the sub plan
     * @param operatorNode: logical operator to be appended
     * @return true if operation succeeds
     */
    bool appendOperatorToSubPlan(uint64_t subPlanId, OperatorNodePtr operatorNode);

    /**
     * Add configuration value to the execution node
     * @param configName: name of the configuration
     * @param configValue: value of the configuration
     * @return true if operation succeeds
     */
    bool addConfiguration(std::string configName, std::string configValue);

    /**
     * Remove the configuration value
     * @param configName: name of the configuration
     * @return true if operation succeeds
     */
    bool removeConfiguration(std::string configName);

    /**
     * update configuration value to the execution node
     * @param configName: name of the configuration
     * @param updatedValue: updated value of the configuration
     * @return true if operation succeeds
     */
    bool updateConfiguration(std::string configName, std::string updatedValue);

  private:
    /**
     * Execution node id
     */
    uint64_t id;

    /**
     * Physical Node information
     */
    NESTopologyEntryPtr nesNode;

    /**
     * list of queryPlans
     */
    std::map<uint64_t, QueryPlanPtr> mapOfQuerySubPlans;

    /**
     * Map of additional configurations
     */
    std::map<std::string, std::string> configurations;
};
}// namespace NES

#endif//NES_EXECUTIONNODE_H
