#ifndef INCLUDE_ACTORS_WORKERACTOR_HPP_
#define INCLUDE_ACTORS_WORKERACTOR_HPP_
#include <Topology/NESTopologyEntry.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/Operator.hpp>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <utility>
#include <caf/blocking_actor.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <QueryCompiler/QueryCompiler.hpp>

using namespace caf;
using std::string;
using std::tuple;
using std::vector;

namespace NES {

// the client queues pending tasks
struct WorkerState {
    strong_actor_ptr current_server;
    std::shared_ptr<NodeEngine> nodeEngine;
};

/**
 * @brief this class handles the actor implementation of the worker
 * class-based, statically typed, event-based API
 * Limitations:
 *  - TODO: this does not handle connection lost
 */
class WorkerActor : public stateful_actor<WorkerState> {
  public:

    /**
     * @brief the constructor to  of the worker to initialize the default objects
     * @param actor config
     * @param ip of this worker
     * @param publish port of this worker
     * @param receive port of this worker
     */
    explicit WorkerActor(actor_config& cfg, string ip, uint16_t publish_port,
                         uint16_t receive_port, NESNodeType type);

  private:
    friend class NesWorker;
    friend class NesCoordinator;

    behavior_type make_behavior() override {
        return init();
    }

    /**
     * @brief this methods registers a physical stream via the coordinator to a logical stream
     * @param configuration of the stream
     * @return bool indicating success
     */
    bool registerPhysicalStream(PhysicalStreamConfig conf);

    /**
     * @brief this method registers logical streams via the coordinator
     * @param name of new logical stream
     * @param path to the file containing the schema
     * @return bool indicating the success of the log stream
     * @note the logical stream is not saved in the worker as it is maintained on the coordinator and all logical streams can be retrieved from the physical stream map locally, if we later need the data we can add a map
     */
    bool registerLogicalStream(std::string streamName, std::string filePath);

    /**
     * @brief this method removes the logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool removeLogicalStream(std::string streamName);

    /**
     * @brief this method removes a physical stream from a logical stream in the coordinator
     * @param logical stream to be deleted
     * @return bool indicating success of the removal
     */
    bool removePhysicalStream(std::string logicalStreamName,
                              std::string physicalStreamName);

    /**
     * @brief @brief method to add a new parent to an existing node
     * @param newParentId
     * @return bool indicating success
     */
    bool addNewParentToSensorNode(std::string parentId);

    /**
     * @brief method to remove a parrent from a node
     * @param newParentId
     * @return bool indicating success
     */
    bool removeParentFromSensorNode(std::string parentId);

    /**
     * @brief method to register a node after the connection is established
     * @param type of the node
     * @return bool indicating success
     */
    bool registerNode(NESNodeType type);

    /**
     * @brief method to get own id form server
     * @return own id as listed in the graph
     */
    size_t getId();

    /**
     * @brief method to connect to the coordinator via caf
     * @param host as address of coordinator
     * @param port as the open port on the coordinaotor
     * @return bool indicating success
     */
    bool connecting(const std::string& host, uint16_t port);

    /**
     * @brief this method disconnect the worker from the coordinator
     * @return bool indicating success
     */
    bool disconnecting();

    /**
     * @brief method to shutdown the worker actor
     * @param if force == true all queries will be stopped, if not false will returned in that case
     * @return bool indicating success
     */
    bool shutdown(bool force);


    /**
    * @brief method to return the query statistics
    * @param id of the query
    * @return queryStatistics
    */
    QueryStatisticsPtr getQueryStatistics(std::string queryId);

  private:
    //states of the actor
    behavior init();
    behavior unconnected();
    behavior running();

    NESNodeType type;
    size_t workerId;
    QueryCompilerPtr queryCompiler;

};

}

#endif //INCLUDE_ACTORS_WORKERACTOR_HPP_
