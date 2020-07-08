#ifndef NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
#define NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_

#include <GRPC/ExecutableTransferObject.hpp>
#include <Sources/DataSource.hpp>

#include <map>
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

class QueryDeployer {

  public:
    QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager, GlobalExecutionPlanPtr globalExecutionPlan);

    ~QueryDeployer();

    /**
     * @brief prepare for a query deployment
     * @param query a queryId of the query
     */
    bool prepareForDeployment(const string& queryId);

    /**
     * @brief helper method to get all sources in a serialized format from a specific node in the topology
     * @param schema the schema
     * @param executionNode the execution node
     * @param execPlan the execution plan
     */
    std::vector<DataSourcePtr> getSources(const std::string& queryId, const ExecutionNodePtr executionNode);

    /**
     * @brief helper method to get all sinks in a serialized format from a specific node in the topology
     * @param queryId
     * @param executionNode execution node
     * @return DataSinkPtr
     */
    std::vector<DataSinkPtr> getSinks(const std::string& queryId, const ExecutionNodePtr executionNode);

    /**
     * @brief find the sink operator starting from the child operator.
     *    IF the child operator has no parent AND the type is of SinkOperator THEN
     *        return the child's DataSinkPtr
     *    ELSE IF the child has a parent THEN
     *        find the parent of child and call findDataSinkPointer method
     *    ELSE
     *        return nullptr and register a WARN message
     * @param operatorPtr
     * @return Data Sink pointer
     */
    DataSinkPtr findDataSinkPointer(OperatorPtr operatorPtr);

    /**
   * @brief find the source operator starting from the child operator.
   *    IF the child operator has no further children AND the type is of SourceOperator THEN
   *        return the child's DataSourcePtr
   *    ELSE IF the child has children THEN
   *        find the children list and call for each child findDataSourcePointer method
   *    ELSE
   *        return nullptr and register a WARN message
   * @param operatorPtr
   * @return Data Source pointer
   */
    DataSourcePtr findDataSourcePointer(OperatorPtr operatorPtr);

    /**
     * @brief helper method to get an automatically assigned receive port where ZMQs are communicating.
     * Currently only server/client architecture, i.e., only one layer, is supported
     * @param query the descriptor of the query
     */
    int assignPort(const string& queryId);

  private:
    std::unordered_map<string, int> queryToPort;
    QueryCatalogPtr queryCatalog;
    TopologyManagerPtr topologyManager;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

}// namespace NES

#endif//NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
