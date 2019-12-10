#include <Actors/ActorCoordinator.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/atom_utils.hpp>
#include <Topology/FogTopologyManager.hpp>

using namespace iotdb;

behavior actor_coordinator::init() {
  initializeNESTopology();

  auto kRootNode = FogTopologyManager::getInstance().getRootNode();
  this->state.actorTopologyMap.insert({this->address().get(), kRootNode});
  this->state.topologyActorMap.insert({kRootNode, this->address().get()});

  // transition to `unconnected` on server failure
  this->set_down_handler([=](const down_msg &dm) {
    strong_actor_ptr key = dm.source.get();
    auto hdl = actor_cast<actor>(key);
    if (this->state.actorTopologyMap.find(key) != this->state.actorTopologyMap.end()) {
      // remove disconnected worker from topology
      this->state.coordinatorServicePtr->deregister_sensor(this->state.actorTopologyMap.at(key));
      this->state.topologyActorMap.erase(this->state.actorTopologyMap.at(key));
      this->state.actorTopologyMap.erase(key);
      aout(this) << "ACTORCOORDINATOR: Lost connection to worker " << key << endl;
      key->get()->unregister_from_system();
    }
  });
  return running();
}

void actor_coordinator::initializeNESTopology() {

  FogTopologyManager::getInstance().resetFogTopologyPlan();
  auto coordinatorNode = FogTopologyManager::getInstance().createFogCoordinatorNode(actorCoordinatorConfig.ip, CPUCapacity::HIGH);
  coordinatorNode->setPublishPort(actorCoordinatorConfig.publish_port);
  coordinatorNode->setReceivePort(actorCoordinatorConfig.receive_port);
}

behavior actor_coordinator::running() {
  return {
      // coordinator specific methods
      [=](register_sensor_atom, string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
          const string &sensor_type) {
        // rpc to register sensor
        this->register_sensor(ip, publish_port, receive_port, cpu, sensor_type);
      },
      [=](register_query_atom, const string &description, const string &strategy) {
        // rpc to register queries
        this->state.coordinatorServicePtr->register_query(description, strategy);
      },
      [=](deregister_query_atom, const string &description) {
        // rpc to unregister a registered query
        this->deregister_query(description);
      },
      [=](deploy_query_atom, const string &description) {
        // rpc to deploy queries to all corresponding actors
        this->deploy_query(description);
      },

      // external methods for users
      [=](topology_json_atom) {
        // print the topology
        string topo = this->state.coordinatorServicePtr->getTopologyPlanString();
        aout(this) << "ACTORCOORDINATOR: Printing Topology" << endl;
        aout(this) << topo << endl;
      },
      [=](show_registered_atom) {
        // print registered queries
        aout(this) << "ACTORCOORDINATOR: Printing Registered Queries" << endl;
        for (const auto &p : this->state.coordinatorServicePtr->getRegisteredQueries()) {
          aout(this) << p.first << endl;
        }
      },
      [=](show_running_atom) {
        // print running queries
        aout(this) << "ACTORCOORDINATOR: Printing Running Queries" << endl;
        for (const auto &p : this->state.coordinatorServicePtr->getRunningQueries()) {
          aout(this) << p.first << endl;
        }
      },
      [=](show_running_operators_atom) {
        // print running operators in the whole topology
        aout(this) << "ACTORCOORDINATOR: Requesting deployed operators from connected devices.." << endl;
        this->show_operators();
      },

      //worker specific methods
      [=](execute_query_atom, const string &description, string &executableTransferObject) {
        // internal rpc to execute a query
        this->state.workerServicePtr->execute_query(description, executableTransferObject);
      },
      // internal rpc to unregister a query
      [=](delete_query_atom, const string &query) {
        this->state.workerServicePtr->delete_query(query);
      },
      // internal rpc to execute a query
      [=](get_operators_atom) {
        return this->state.workerServicePtr->getOperators();
      }
  };
}

void actor_coordinator::register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                                        const string &sensor) {
  auto sap = current_sender();
  auto hdl = actor_cast<actor>(sap);
  FogTopologyEntryPtr
      sensorNode = this->state.coordinatorServicePtr->register_sensor(ip, publish_port, receive_port, cpu, sensor);

  this->state.actorTopologyMap.insert({sap, sensorNode});
  this->state.topologyActorMap.insert({sensorNode, sap});
  this->monitor(hdl);
  IOTDB_INFO("ACTORCOORDINATOR: Successfully registered sensor (CPU=" << cpu << ", Type: " << sensor << ") "
                                                                      << to_string(hdl));
}

void actor_coordinator::deploy_query(const string &queryString) {
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject>
      deployments = this->state.coordinatorServicePtr->make_deployment(queryString);

  for (auto const &x : deployments) {
    strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
    auto hdl = actor_cast<actor>(sap);
    string s_eto = SerializationTools::ser_eto(x.second);
    IOTDB_INFO("ACTORCOORDINATOR: Sending query " << queryString << " to " << to_string(hdl));
    this->request(hdl, task_timeout, execute_query_atom::value, queryString, s_eto);
  }
}

/**
 * @brief method which is called to unregister an already running query
 * @param queryId the queryId of the query
 */
void actor_coordinator::deregister_query(const string &queryId) {
  // send command to all corresponding nodes to stop the running query as well
  for (auto const &x : this->state.actorTopologyMap) {
    auto hdl = actor_cast<actor>(x.first);
    IOTDB_INFO("ACTORCOORDINATOR: Sending deletion request " << queryId << " to " << to_string(hdl));
    this->request(hdl, task_timeout, delete_query_atom::value, queryId);
  }
  this->state.coordinatorServicePtr->deregister_query(queryId);
}

/**
* @brief send messages to all connected devices and get their operators
*/
void actor_coordinator::show_operators() {
  for (auto const &x : this->state.actorTopologyMap) {
    strong_actor_ptr sap = x.first;
    auto hdl = actor_cast<actor>(sap);

    this->request(hdl, task_timeout, get_operators_atom::value).then(
        [=](const vector<string> &vec) {
          std::ostringstream ss;
          ss << x.second->getEntryTypeString() << "::" << to_string(hdl) << ":" << endl;

          aout(this) << ss.str() << vec << endl;
        },
        [=](const error &er) {
          string error_msg = to_string(er);
          IOTDB_ERROR("ACTORCOORDINATOR: Error during show_operators for " << to_string(hdl) << "\n" << error_msg);
        }
    );
  }
}
