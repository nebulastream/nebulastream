#ifndef INCLUDE_ACTORS_WORKERSERVICE_HPP_
#define INCLUDE_ACTORS_WORKERSERVICE_HPP_

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/QueryCompiler.hpp>
#include <string>
#include <tuple>
#include <vector>

using std::string;
using std::tuple;
using std::vector;
using JSON = nlohmann::json;

namespace NES {

class WorkerService {

    enum NodeQueryStatus {
        started,
        stopped,
        deployed
    };

  public:
    ~WorkerService() = default;

    /**
     * @brief constructor for worker with a stream/sensor attached and create default physical stream
     * NOTE: the mutual start/depoloy and stop/undeploy leads to underterministic behavior and thus will be permitted
     */
    WorkerService(string ip, uint16_t publish_port, uint16_t receive_port);

    /**
     * @brief method to just deploy a query without starting it
     * @param queryId a queryId of the query
     * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
     * @return bool indicating success
     */
    bool deployQuery(const std::string& queryId, std::string& executableTransferObject);

    /**
     * @brief method to undeploy a query not runining query
     * @note if query is still running, false is returned
     * @param queryId
     * @return bool indicating success
     */
    bool undeployQuery(const std::string& queryId);

    /**
     * @brief method to start a already deployed query
     * @note if query is not deploy, false is returned
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(const std::string& queryId);

    /**
     * @brief method to stop a query
     * @param queryId
     * @return bool indicating success
     */
    bool stopQuery(const std::string& queryId);

    /**
     * @brief getter/setter for IP
     * @return
     */
    string& getIp();
    void setIp(const string& ip);

    /**
     * @brief getter/sett for publish port
     * @return
     */
    uint16_t getPublishPort() const;
    void setPublishPort(uint16_t publish_port);

    /**
     * @brief getter/setter for receive port
     * @return
     */
    uint16_t getReceivePort() const;
    void setReceivePort(uint16_t receive_port);

    /**
     * @brief method to get the node propertiers
     * @return node properties as a string
     */
    string getNodeProperties();

    /**
     * @brief method to add the pyhsical stream that this node produce
     * @param conf of physical stream
     */
    void addPhysicalStreamConfig(PhysicalStreamConfig conf);

    /**
    * @brief method to shut down the worker service, including stop engine and empty all queues
    */
    void shutDown();

  private:
    string ip;
    uint16_t publishPort;
    uint16_t receivePort;
    QueryCompilerPtr queryCompiler;

    std::map<std::string, PhysicalStreamConfig> physicalStreams;

    std::shared_ptr<NodeEngine> nodeEngine;

    std::unordered_map<string, QueryExecutionPlanPtr> queryIdToQEPMap;
    std::unordered_map<string, NodeQueryStatus> queryIdToStatusMap;
};

} // namespace NES

#endif // INCLUDE_ACTORS_WORKERSERVICE_HPP_
