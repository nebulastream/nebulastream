#include <Actors/CoordinatorActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include "../../include/Topology/NESTopologyManager.hpp"

namespace iotdb {

  behavior CoordinatorActor::init() {
      initializeNESTopology();

      auto kRootNode = NESTopologyManager::getInstance().getRootNode();
      this->state.actorTopologyMap.insert({this->address().get(), kRootNode});
      this->state.topologyActorMap.insert({kRootNode, this->address().get()});

      // transition to `unconnected` on server failure
      this->set_down_handler([=](const down_msg& dm) {
        strong_actor_ptr key = dm.source.get();
        auto hdl = actor_cast<actor>(key);
        if (this->state.actorTopologyMap.find(key) != this->state.actorTopologyMap.end()) {
            // remove disconnected worker from topology
            coordinatorServicePtr->deregister_sensor(this->state.actorTopologyMap.at(key));
            this->state.topologyActorMap.erase(this->state.actorTopologyMap.at(key));
            this->state.actorTopologyMap.erase(key);
            aout(this) << "ACTORCOORDINATOR: Lost connection to worker " << key << endl;
            key->get()->unregister_from_system();
        }
      });
      return running();
  }

  void CoordinatorActor::initializeNESTopology() {

      NESTopologyManager::getInstance().resetFogTopologyPlan();
      auto coordinatorNode =
          NESTopologyManager::getInstance().createFogCoordinatorNode(actorCoordinatorConfig.ip, CPUCapacity::HIGH);
      coordinatorNode->setPublishPort(actorCoordinatorConfig.publish_port);
      coordinatorNode->setReceivePort(actorCoordinatorConfig.receive_port);
  }

  behavior CoordinatorActor::running() {
      return {
          // coordinator specific methods
          [=](register_sensor_atom, string& ip, uint16_t publish_port, uint16_t receive_port, int cpu,
              const string& sensor_type, const string& nodeProperties) {
            this->registerSensor(ip, publish_port, receive_port, cpu, sensor_type, nodeProperties);
          },
          [=](execute_query_atom, const string& description, const string& strategy) {
            return executeQuery(description, strategy);
          },
          [=](register_query_atom, const string& description, const string& strategy) {
            return registerQuery(description, strategy);
          },
          [=](deregister_query_atom, const string& description) {
            this->deregisterQuery(description);
          },
          [=](deploy_query_atom, const string& description) {
            this->deployQuery(description);
          },

          //worker specific methods
          [=](execute_operators_atom, const string& description, string& executableTransferObject) {
            workerServicePtr->execute_query(description, executableTransferObject);
          },
          [=](delete_query_atom, const string& query) {
            workerServicePtr->delete_query(query);
          },
          [=](get_operators_atom) {
            return workerServicePtr->getOperators();
          },

          // external methods for users
          [=](topology_json_atom) {
            string topo = coordinatorServicePtr->getTopologyPlanString();
            aout(this) << "Printing Topology" << endl;
            aout(this) << topo << endl;
          },
          [=](show_registered_atom) {
            aout(this) << "Printing Registered Queries" << endl;
            for (const auto& p : coordinatorServicePtr->getRegisteredQueries()) {
                aout(this) << p.first << endl;
            }
            return coordinatorServicePtr->getRegisteredQueries().size();
          },
          [=](show_running_atom) {
            aout(this) << "Printing Running Queries" << endl;
            for (const auto& p : coordinatorServicePtr->getRunningQueries()) {
                aout(this) << p.first << endl;
            }
          },
          [=](show_running_operators_atom) {
            aout(this) << "Requesting deployed operators from connected devices.." << endl;
            this->showOperators();
          }
      };
  }

  void CoordinatorActor::registerSensor(const string& ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                                        const string& sensor, const string& nodeProperties) {
      auto sap = current_sender();
      auto hdl = actor_cast<actor>(sap);
      NESTopologyEntryPtr
          sensorNode = coordinatorServicePtr->register_sensor(ip, publish_port, receive_port, cpu, sensor, nodeProperties);

      this->state.actorTopologyMap.insert({sap, sensorNode});
      this->state.topologyActorMap.insert({sensorNode, sap});
      this->monitor(hdl);
      IOTDB_INFO("ACTORCOORDINATOR: Successfully registered sensor (CPU=" << cpu << ", Type: " << sensor << ") "
                                                                          << to_string(hdl));
  }

  void CoordinatorActor::deployQuery(const string& queryId) {
      unordered_map<NESTopologyEntryPtr, ExecutableTransferObject>
          deployments = coordinatorServicePtr->make_deployment(queryId);

      for (auto const& x : deployments) {
          strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
          auto hdl = actor_cast<actor>(sap);
          string s_eto = SerializationTools::ser_eto(x.second);
          IOTDB_INFO("Sending query " << queryId << " to " << to_string(hdl));
          this->request(hdl, task_timeout, execute_operators_atom::value, queryId, s_eto);
      }
  }

  void CoordinatorActor::deregisterQuery(const string& queryId) {
      // send command to all corresponding nodes to stop the running query as well
      for (auto const& x : this->state.actorTopologyMap) {
          auto hdl = actor_cast<actor>(x.first);
          IOTDB_INFO("ACTORCOORDINATOR: Sending deletion request " << queryId << " to " << to_string(hdl));
          this->request(hdl, task_timeout, delete_query_atom::value, queryId);
      }
      coordinatorServicePtr->deregister_query(queryId);
  }

  void CoordinatorActor::showOperators() {
      for (auto const& x : this->state.actorTopologyMap) {
          strong_actor_ptr sap = x.first;
          auto hdl = actor_cast<actor>(sap);

          this->request(hdl, task_timeout, get_operators_atom::value).then(
              [=](const vector<string>& vec) {
                std::ostringstream ss;
                ss << x.second->getEntryTypeString() << "::" << to_string(hdl) << ":" << endl;

                aout(this) << ss.str() << vec << endl;
              },
              [=](const error& er) {
                string error_msg = to_string(er);
                IOTDB_ERROR("ACTORCOORDINATOR: Error during showOperators for " << to_string(hdl) << "\n" << error_msg);
              }
          );
      }
  }

  string CoordinatorActor::registerQuery(const string& queryString, const string& strategy) {
      return coordinatorServicePtr->register_query(queryString, strategy);
  }

  string CoordinatorActor::executeQuery(const string& queryString, const string& strategy) {
      string queryId = coordinatorServicePtr->register_query(queryString, strategy);
      deployQuery(queryId);
      return queryId;
  }

}
