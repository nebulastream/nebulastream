#ifndef INCLUDE_ACTORS_COORDINATORSERVICE_HPP_
#define INCLUDE_ACTORS_COORDINATORSERVICE_HPP_

#include <SourceSink/DataSource.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <API/InputQuery.hpp>

#include <API/UserAPIExpression.hpp>

#include <Util/UtilityFunctions.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <Operators/Impl/SourceOperator.hpp>
#include <Operators/Impl/SinkOperator.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Services/OptimizerService.hpp>
#include <Services/QueryService.hpp>
#include <Util/UtilityFunctions.hpp>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Optimizer/NESOptimizer.hpp>
#include <SourceSink/CSVSource.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <cstdint>
#include <string>
#include <Optimizer/ExecutionGraph.hpp>

using std::string;

namespace NES {

class CoordinatorService;
typedef std::shared_ptr<CoordinatorService> CoordinatorServicePtr;

class CoordinatorService {

  public:
    static CoordinatorServicePtr getInstance() {
        static CoordinatorServicePtr instance{new CoordinatorService};
        return instance;
    }

    ~CoordinatorService() = default;

    /**
     * @brief registers a CAF worker into the NES topology and creates a corresponding NESTopologyWorkerNode object
     * @param id of the node
     * @param ip the worker ip
     * @param publish_port the publish port of the worker
     * @param receive_port the receive port of the worker
     * @param cpu the cpu capacity of the worker
     * @param nodeProperties of the to be added sensor
     * @param config of the node
     * @param node type
     * @return handle to the node
     */
    NESTopologyEntryPtr registerNode(size_t id,
                                     const string& ip,
                                     uint16_t publish_port,
                                     uint16_t receive_port,
                                     int cpu,
                                     const string& nodeProperties,
                                     PhysicalStreamConfig streamConf,
                                     NESNodeType type);

    /**
     * @brief @brief add a new parent to an existing node
     * @param childId
     * @param newParrentId
     * @return bool indicating success
     */
    bool addNewParentToSensorNode(size_t childId, size_t newParentId);

    /**
     * @brief remove a parrent from a node
     * @param childId
     * @param newParrentId
     * @return bool indicating succesws
     */
    bool removeParentFromSensorNode(size_t childId, size_t newParentId);

    /**
     * @brief deregisters a CAF worker from the NES topology
     * @param entry
     * @return true, if it succeeded, otherwise false
     */
    bool deregisterSensor(const NESTopologyEntryPtr entry);

    /**
     * @brief registers a CAF query into the NES topology to make it deployable
     * @param queryString a queryString of the query
     * @param optimizationStrategyName the optimization strategy (buttomUp or topDown)
     */
    string registerQuery(const string& queryString, const string& optimizationStrategyName);

    /**
     * @brief method which is called to unregister an already running query
     * @param queryId the queryId of the query
     * @return true if deleted from running queries, otherwise false
     */
    bool deleteQuery(const string& queryId);

    /**
     * @brief deploys a CAF query into the NES topology to the corresponding devices defined by the optimizer
     * @param query a queryId of the query
     */
    map<NESTopologyEntryPtr, ExecutableTransferObject> prepareExecutableTransferObject(const string& queryId);

    /**
     * @brief creates a string representation of the topology graph
     * @return the topology as string representation
     */
    string getTopologyPlanString();

    /**
     * @brief return the properties of a particular node
     * @param entry in the topology
     * @return string containing the properties
     */
    string getNodePropertiesAsString(const NESTopologyEntryPtr entry);

    //FIXME: right now we do not register query but rather the nes plan
    /**
     * @brief: get the registered query
     * @param queryId
     * @return the nes execution plan for the query
     */
    NESExecutionPlanPtr getRegisteredQuery(string queryId);

    /**
     * @brief: clear query catalogs
     * @return
     */
    bool clearQueryCatalogs();

    /**
     * @brief method to shut down the coordinator service
     * @return bool indicating success
     */
    bool shutdown();

    /**
     * @brief method to return currently registered queries
     * @return map containing the query id and the link to the entry in the query catalog
     */
    const map<string, QueryCatalogEntryPtr> getRegisteredQueries();

    /**
     * @brief method to return currently running queries
     * @return map containing the query id and the link to the entry in the query catalog
     */
    const map<string, QueryCatalogEntryPtr> getRunningQueries();

  private:
    CoordinatorService() = default;//do not implement

    unordered_map<string, int> queryToPort;
    shared_ptr<NESTopologyManager> topologyManagerPtr;

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
};

}
#endif //INCLUDE_ACTORS_COORDINATORSERVICE_HPP_
