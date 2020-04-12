#ifndef INCLUDE_ACTORS_COORDINATORACTOR_HPP_
#define INCLUDE_ACTORS_COORDINATORACTOR_HPP_

#include <caf/io/all.hpp>
#include <caf/all.hpp>
#include <Actors/AtomUtils.hpp>
#include <Services/CoordinatorService.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/WorkerService.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <API/Schema.hpp>
#include <Util/Logger.hpp>

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

namespace NES {

// class-based, statically typed, event-based API for the state management in CAF
struct CoordinatorState {
    unordered_map<caf::strong_actor_ptr, NESTopologyEntryPtr> actorTopologyMap;
    unordered_map<NESTopologyEntryPtr, caf::strong_actor_ptr> topologyActorMap;
    unordered_map<size_t, caf::strong_actor_ptr> idToActorMap;
};

/**
 * @brief the coordinator for NES
 */
class CoordinatorActor : public caf::stateful_actor<CoordinatorState> {

  public:
    /**
     * @brief the constructor of the coordinator to initialize the default objects
     */

    explicit CoordinatorActor(caf::actor_config& cfg)
        :
        stateful_actor(cfg) {

        queryCatalogServicePtr = QueryCatalogService::getInstance();
        streamCatalogServicePtr = StreamCatalogService::getInstance();
        coordinatorServicePtr = CoordinatorService::getInstance();
        coordinatorIp = "localhost";
        workerServicePtr = std::make_unique<WorkerService>(
            WorkerService(actorCoordinatorConfig.ip, actorCoordinatorConfig.publish_port,
                          actorCoordinatorConfig.receive_port));
    }

    explicit CoordinatorActor(caf::actor_config& cfg, std::string ip)
        :
        stateful_actor(cfg), coordinatorIp(ip) {

        queryCatalogServicePtr = QueryCatalogService::getInstance();
        streamCatalogServicePtr = StreamCatalogService::getInstance();
        coordinatorServicePtr = CoordinatorService::getInstance();
        workerServicePtr = std::make_unique<WorkerService>(
            WorkerService(ip, actorCoordinatorConfig.publish_port,
                          actorCoordinatorConfig.receive_port));
    }

    ~CoordinatorActor();

    behavior_type make_behavior() override {
        return init();
    }

  private:
    caf::behavior init();
    caf::behavior running();

    /**
     * @brief method to add a physical stream to the catalog AND to the topology
     * @caution every external register call can only register streams for himself
     * @caution we will deploy on the first worker with that ip NOT on all
     * @param config of the physical stream
     * @return bool indicating if removal was successful
     */
    bool registerPhysicalStream(PhysicalStreamConfig streamConf);

    /**
     * @brief method to remove a physical stream from the catalog AND from the topology
     * @caution every external register call can only remove streams from himself
     * @caution we will remove only on the first node that we find for this logical stream
     * @param logicalStreamName as string
     * @param physicalStreamName as string
     * @return bool indicating if removal was successful
     */
    bool removePhysicalStream(string logicalStreamName,
                              string physicalStreamName);

    /**
     * @brief @brief method to add a new parent to an existing node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool addNewParentToSensorNode(std::string childId, std::string parentId);

    /**
     * @brief method to remove a parent from a node
     * @param childId
     * @param parentId
     * @return bool indicating success
     */
    bool removeParentFromSensorNode(std::string childId, std::string parentId);

    /**
   * @brief : registering a new sensor node
   * @param ip
   * @param publish_port
   * @param receive_port
   * @param cpu
   * @param properties of this worker
   * @param configuration of the sensor
   */
    bool registerSensor(const string& ip, uint16_t publish_port,
                        uint16_t receive_port, int cpu,
                        const string& nodeProperties,
                        PhysicalStreamConfig streamConf);

    /**
     * @brief: remove a sensor node from topology and catalog
     * @caution: if there is more than one, potential node to delete, an exception is thrown
     * @param ip
     */
    bool deregisterSensor(const string& ip);

    /**
     * @brief execute user query will first register the query and then deploy it.
     * @param queryString : user query, in string form, to be executed
     * @param strategy : deployment strategy for the query operators
     * @return UUID of the submitted user query.
     */
    string executeQuery(const string& queryString, const string& strategy);

    /**
     * @brief register the user query
     * @param queryString string representation of the query
     * @return uuid of the registered query
     */
    string registerQuery(const string& queryString, const string& strategy);

    /**
     * @brief: deploy the user query
     * @param queryId
     */
    void deployQuery(const string& queryId);

    /**
     * @brief method which is called to unregister an already running query
     * @param queryId of the query to be de-registered
     * @bool indicating the success of the disconnect
     */
    void deregisterQuery(const string& queryId);

    /**
     * @brief send messages to all connected devices and get their operators
     */
    void showOperators();

    /**
     * @brief initialize the NES topology and add coordinator node
     */
    void initializeNESTopology();

    void shutdown();

    /**
     * @brief method to get own id
     * @return id that is listed in graph
     */
    string getOwnId();

    std::string coordinatorIp;
    QueryCatalogServicePtr queryCatalogServicePtr;
    StreamCatalogServicePtr streamCatalogServicePtr;
    CoordinatorActorConfig actorCoordinatorConfig;
    CoordinatorServicePtr coordinatorServicePtr;
    std::unique_ptr<WorkerService> workerServicePtr;
};
}

#endif //INCLUDE_ACTORS_COORDINATORACTOR_HPP_
