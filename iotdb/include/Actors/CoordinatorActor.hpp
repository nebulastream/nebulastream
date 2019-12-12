#ifndef IOTDB_INCLUDE_ACTORS_COORDINATORACTOR_HPP_
#define IOTDB_INCLUDE_ACTORS_COORDINATORACTOR_HPP_

#include <caf/io/all.hpp>
#include <caf/all.hpp>
#include <Actors/AtomUtils.hpp>
#include <Topology/FogTopologyEntry.hpp>
#include <Services/CoordinatorService.hpp>
#include <Services/WorkerService.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Actors/Configurations/CoordinatorActorConfig.hpp>
#include <Topology/FogTopologyManager.hpp>

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

namespace iotdb {

  // class-based, statically typed, event-based API for the state management in CAF
  struct CoordinatorState {
      unordered_map<caf::strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
      unordered_map<FogTopologyEntryPtr, caf::strong_actor_ptr> topologyActorMap;
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
          : stateful_actor(cfg) {
          coordinatorServicePtr = CoordinatorService::getInstance();
          workerServicePtr = std::make_unique<WorkerService>(WorkerService(actorCoordinatorConfig.ip,
                                                                           actorCoordinatorConfig.publish_port,
                                                                           actorCoordinatorConfig.receive_port,
                                                                           ""));
      }

      behavior_type make_behavior() override {
          return init();
      }

    private:
      caf::behavior init();
      caf::behavior running();

      /**
       * @brief : registering a new sensor node
       *
       * @param ip
       * @param publish_port
       * @param receive_port
       * @param cpu
       * @param sensor
       */
      void registerSensor(const string& ip,
                          uint16_t publish_port,
                          uint16_t receive_port,
                          int cpu,
                          const string& sensor);

      /**
       * @brief register the user query
       * @param queryString string representation of the query
       * @return uuid of the registered query
       */
      string registerQuery(const string& queryString, const string& strategy);

      /**
       * @brief: deploy the user query
       *
       * @param queryId
       */
      void deployQuery(const string& queryId);

      /**
       * @brief method which is called to unregister an already running query
       *
       * @param queryId of the query to be de-registered
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

      CoordinatorActorConfig actorCoordinatorConfig;
      CoordinatorServicePtr coordinatorServicePtr;
      std::unique_ptr<WorkerService> workerServicePtr;
  };
}

#endif //IOTDB_INCLUDE_ACTORS_COORDINATORACTOR_HPP_
