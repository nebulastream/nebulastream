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
    explicit CoordinatorActor(caf::actor_config& cfg, std::string ip);

    ~CoordinatorActor();
  private:
    behavior_type make_behavior() override {
        return init();
    }

    friend class NesCoordinator;
    friend class QueryController;
    friend class QueryCatalogController;

    caf::behavior init();
    caf::behavior running();

    /**
     * @brief method to add a physical stream to the catalog AND to the topology
     * @caution every external register call can only register streams for himself
     * @caution we will deploy on the first worker with that ip NOT on all
     * @param id of the worker
     * @param config of the physical stream
     * @return bool indicating if removal was successful
     */
    bool registerPhysicalStream(size_t workerId, PhysicalStreamConfig streamConf);

    /**
     * @brief method to remove a physical stream from the catalog AND from the topology
     * @caution every external register call can only remove streams from himself
     * @caution we will remove only on the first node that we find for this logical stream
     * @param id of the worker
     * @param logicalStreamName as string
     * @param physicalStreamName as string
     * @return bool indicating if removal was successful
     */
    bool removePhysicalStream(size_t workerId, string logicalStreamName,
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
   * @brief : registering a new node
   * @param ip
   * @param publish_port
   * @param receive_port
   * @param cpu
   * @param properties of this worker
   * @param configuration of the sensor
   * @param node type
   * @return id of the node, 0 if a failure occurs
   */
    size_t registerNode(const string& ip, uint16_t publish_port,
                      uint16_t receive_port, int cpu,
                      const string& nodeProperties,
                      PhysicalStreamConfig streamConf,
                      NESNodeType type);

    /**
     * @brief: remove a sensor node from topology and catalog
     * @caution: if there is more than one, potential node to delete, an exception is thrown
     * @param id of the worker
     * @return bool indicating success
     */
    bool deregisterNode(size_t workerId);

    /**
     * @brief execute user query will first register the query and then deploy it.
     * @param id of the worker
     * @param queryString : user query, in string form, to be executed
     * @param strategy : deployment strategy for the query operators
     * @return UUID of the submitted user query.
     */
    string registerAndDeployQuery(size_t workerId, const string& queryString, const string& strategy);

    /**
     * @brief register the user query
     * @param id of the worker
     * @param queryString string representation of the query
     * @return uuid of the registered query
     */
    string registerQuery(size_t workerId, const string& queryString, const string& strategy);

    /**
     * @brief: deploy the user query, which includes sending and starting
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(size_t workerId, const string& queryId);

    /**
     * @brief method which is called to unregister an already running query
     * @param queryId of the query to be de-registered
     * @bool indicating the success of the disconnect
     */
    bool deregisterAndUndeployQuery(size_t workerId, const string& queryId);

    /**
     * @brief method to shutdown the actor
     * @return bool indicating success
     */
    bool shutdown();

    std::string coordinatorIp;
    QueryCatalogServicePtr queryCatalogServicePtr;
    StreamCatalogServicePtr streamCatalogServicePtr;
    CoordinatorActorConfig actorCoordinatorConfig;
    CoordinatorServicePtr coordinatorServicePtr;
};
}

#endif //INCLUDE_ACTORS_COORDINATORACTOR_HPP_
