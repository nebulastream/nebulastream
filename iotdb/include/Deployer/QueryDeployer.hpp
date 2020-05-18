#ifndef NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
#define NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_

#include <GRPC/ExecutableTransferObject.hpp>
#include <Optimizer/ExecutionGraph.hpp>
#include <SourceSink/DataSource.hpp>
#include <Catalogs/QueryCatalog.hpp>

#include <map>
#include <vector>
using namespace std;
namespace NES {
class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class QueryDeployer {

  public:
    QueryDeployer(QueryCatalogPtr queryCatalog, TopologyManagerPtr topologyManager);

    ~QueryDeployer();

    /**
    * @brief generates a deployment for a query
    * @param query a queryId of the query
     * @return map containing the deployment
   */
    map<NESTopologyEntryPtr, ExecutableTransferObject> generateDeployment(const string& queryId);

    /**
     * @brief helper method to get all sources in a serialized format from a specific node in the topology
     * @param schema the schema
     * @param v the execution vertex
     * @param execPlan the execution plan
     */
    vector<DataSourcePtr> getSources(const string& queryId, const ExecutionVertex& v);

    /**
     * @brief helper method to get all sinks in a serialized format from a specific node in the topology
     * @param queryId
     * @param v execution vertex
     * @return DataSinkPtr
     */
    vector<DataSinkPtr> getSinks(const string& queryId, const ExecutionVertex& v);

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
    int assign_port(const string& queryId);

  private:
    unordered_map<string, int> queryToPort;
    QueryCatalogPtr queryCatalog;
    TopologyManagerPtr topologyManager;
};

typedef std::shared_ptr<QueryDeployer> QueryDeployerPtr;

}// namespace NES

#endif//NES_INCLUDE_DEPLOYER_QUERYDEPLOYER_HPP_
