#ifndef INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#define INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_
#include "REST/RestServer.hpp"
#include <string>
#include <Actors/WorkerActor.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>

namespace NES {

class NesCoordinator {
  public:
    /**
     * @brief default constructor
     * @note this will create the coordinator actor using the default coordinator config
     * @limitations
     *    - currently we start rest server always
     */
    NesCoordinator();

    /**
     * @brief start actor: rest server, caf server, coordinator actor
     * @note this will start the server at a random port
     * @param bool if the method should block
     * @return port of where started
     */
    uint16_t startCoordinator(bool blocking);

    /**
     * @brief start actor: rest server, caf server, coordinator actor
     * @param bool if the method should block
     * @param port where to start the serv er
     */
    void startCoordinator(bool blocking, uint16_t port);

    /**
     * @brief method to stop coordinator
     * @return bool indicating success
     */
    bool stopCoordinator();

    /**
     * @brief method to register, deploy, and start a query
     * @param queryString : user query, in string form, to be executed
     * @param strategy : deployment strategy for the query operators
     * @return UUID of the submitted user query.
     */
    string deployQuery(const string& queryString, const string& strategy);

    /**
     * @brief method to deregister, undeploy, and stop a query
     * @param queryString : user query, in string form, to be executed
     * @param strategy : deployment strategy for the query operators
     * @return UUID of the submitted user query.
     */
    bool undeployQuery(const string& queryId);


    /**
     * @brief method to overwrite the default config for the rest server
     * @param host as string
     * @param port as uint
     */
    void setRestConfiguration(std::string host, uint16_t port);

    /**
     * @brief method to get handle
     * @return handle to actor
     */
    infer_handle_from_class_t<CoordinatorActor> getActorHandle();

    void setServerIp(std::string serverIp);

  private:
    CoordinatorActor* crdPtr;
    CoordinatorActorConfig coordinatorCfg;
    infer_handle_from_class_t<CoordinatorActor> coordinatorActorHandle;
    actor_system* actorSystemCoordinator;

    WorkerActor* wrkPtr;
    WorkerActorConfig workerCfg;
    infer_handle_from_class_t<NES::WorkerActor> workerActorHandle;
    actor_system* actorSystemWorker;

    RestServer* restServer;
    std::string restHost;
    std::string serverIp;
    uint16_t restPort;
    uint16_t actorPort;
    std::thread restThread;
};
typedef std::shared_ptr<NesCoordinator> NesCoordinatorPtr;

}
#endif /* INCLUDE_COMPONENTS_NESCOORDINATOR_HPP_ */
