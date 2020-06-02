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
     * Get the query sub plan
     * @param subPlanId: id of the sub plan
     * @return query sub plan
     */
    QueryPlanPtr getSubPlan(uint64_t subPlanId);

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

    const std::string toString() const override;

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

static ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode, uint64_t subPlanId, QueryPlanPtr querySubPlan);
}// namespace NES

#endif//NES_EXECUTIONNODE_H
