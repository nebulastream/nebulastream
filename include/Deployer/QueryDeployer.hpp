#ifndef NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
#define NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_

#include <Sources/DataSource.hpp>

#include <map>
#include <unordered_map>
#include <vector>

namespace NES {

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class NESTopologyEntry;
typedef std::shared_ptr<NESTopologyEntry> NESTopologyEntryPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class Operator;
typedef std::shared_ptr<Operator> OperatorPtr;

class QueryDeployer {

  public:
    QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager, GlobalExecutionPlanPtr globalExecutionPlan);

    ~QueryDeployer();

    /**
     * @brief prepare for a query deployment
     * @param query a queryId of the query
     */
    bool prepareForDeployment(const uint64_t queryId);

    /**
     * @brief helper method to get an automatically assigned receive port where ZMQs are communicating.
     * Currently only server/client architecture, i.e., only one layer, is supported
     * @param query the descriptor of the query
     */
    int assignPort(const uint64_t queryId);

  private:
    std::unordered_map<uint64_t, int> queryToPort;
    QueryCatalogPtr queryCatalog;
    TopologyManagerPtr topologyManager;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

}// namespace NES

#endif//NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
