#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#include "REST/RestServer.hpp"
#include <Actors/Configurations/WorkerActorConfig.hpp>
//#include <Actors/WorkerActor.hpp>
#include <Components/NesWorker.hpp>
#include <Services/CoordinatorService.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/StreamCatalogService.hpp>
#include <string>
#include <grpcpp/grpcpp.h>
#include <GRPC/WorkerRPCClient.hpp>

namespace NES {
typedef map<NESTopologyEntryPtr, ExecutableTransferObject> QueryDeployment;

class NesCoordinator : public std::enable_shared_from_this<NesCoordinator> {
  public:
    /**
     * @brief default constructor
     * @note this will create the coordinator actor using the default coordinator config
     * @limitations
     *    - currently we start rest server always
     */
    NesCoordinator();

    NesCoordinator(string serverIp, uint16_t restPort, uint16_t rpcPort);

    /**
     * @brief dtor
     * @return
     */
    ~NesCoordinator();

    /**
     * @brief start actor: rest server, caf server, coordinator actor
     * @param bool if the method should block
     */
    size_t startCoordinator(bool blocking);

    /**
     * @brief method to stop coordinator
     * @param force the shutdown even when queries are running
     * @return bool indicating success
     */
    bool stopCoordinator(bool force);

    /**
     * @brief method to register, deploy, and start a query
     * @param queryString : user query, in string form, to be executed
     * @param strategy : deployment strategy for the query operators
     * @return UUID of the submitted user query.
     */
    string addQuery(const string queryString, const string strategy);

    /**
     * @brief method to deregister, undeploy, and stop a query
     * @param queryID
     * @return bool indicating success
     */
    bool removeQuery(const string queryId);

    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(std::string queryId);

    /**
     * @brief method to overwrite server ip
     * @param serverIp
     */
    void setServerIp(std::string serverIp);

  private:
    /**
    * @brief method to register a query without start
    * @param queryString : user query, in string form, to be executed
    * @param strategy : deployment strategy for the query operators
    * @return UUID of the submitted user query.
    */
    string registerQuery(const string queryString, const string strategy);

    /**
     * @brief ungregisters a query
     * @param queryID
     * @return bool indicating success
     */
    bool unregisterQuery(const string queryId);

    /**
     * @brief method to start a already deployed query
     * @param queryDeployments
     * @return bool indicating success
     */
    bool startQuery(std::string queryId);

    /**
     * @brief method to stop a query
     * @param queryDeployments
     * @return bool indicating success
     */
    bool stopQuery(std::string queryId);


    /**
     * @brief method send query to nodes
     * @param queryDeployments
     * @return bool indicating success
    */
    bool deployQuery(std::string queryId);

    /**
     * @brief method remove query from nodes
     * @param queryDeployments
     * @return bool indicating success
     */
    bool undeployQuery(std::string queryId);


  private:
    size_t getRandomPort(size_t base);

    std::shared_ptr<grpc::Server> rpcServer;
    uint16_t rpcPort;
    std::thread rpcThread;
    NesWorkerPtr wrk;

    WorkerRPCClientPtr workerRPCClient;

    //    CoordinatorActor* crdActor;
    //    CoordinatorActorConfig coordinatorCfg;
    //    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
    //    actor_system* actorSystemCoordinator;

    //    WorkerActor* wrkActor;
    //    NodeEnginePtr nodeEngine;
    //    WorkerActorConfig workerCfg;
    //    infer_handle_from_class_t<NES::WorkerActor> workerActorHandle;
    //    actor_system* actorSystemWorker;

    RestServer* restServer;
    uint16_t restPort;
    std::string serverIp;

    std::thread restThread;

    std::atomic<bool> stopped;

    QueryCatalogServicePtr queryCatalogServicePtr;
    StreamCatalogServicePtr streamCatalogServicePtr;
    CoordinatorServicePtr coordinatorServicePtr;

    std::map<std::string, QueryDeployment> currentDeployments;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;


}// namespace NES
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */
