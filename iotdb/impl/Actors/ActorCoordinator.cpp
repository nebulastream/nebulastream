#include <Actors/ActorCoordinator.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/atom_utils.hpp>

#include <caf/all.hpp>

namespace iotdb {

behavior actor_coordinator::init() {
  this->state.actorTopologyMap.insert({this->address().get(), this->state.coordinatorPtr->getThisEntry()});
  this->state.topologyActorMap.insert({this->state.coordinatorPtr->getThisEntry(), this->address().get()});

  this->state.enginePtr = new NodeEngine();
  this->state.enginePtr->start();

  // transition to `unconnected` on server failure
  this->set_down_handler([=](const down_msg &dm) {
    strong_actor_ptr key = dm.source.get();
    auto hdl = actor_cast<actor>(key);
    if (this->state.actorTopologyMap.find(key) != this->state.actorTopologyMap.end()) {
      // remove disconnected worker from topology
      this->state.coordinatorPtr->deregister_sensor(this->state.actorTopologyMap.at(key));
      this->state.topologyActorMap.erase(this->state.actorTopologyMap.at(key));
      this->state.actorTopologyMap.erase(key);
      aout(this) << "*** lost connection to worker " << key << endl;
      key->get()->unregister_from_system();
    }
  });
  return running();
}

behavior actor_coordinator::running() {
  return {
      // framework internal rpcs
      [=](register_sensor_atom, string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
          const string &sensor_type, const strong_actor_ptr &sap) {
        // rpc to register sensor
        this->register_sensor(ip, publish_port, receive_port, cpu, sensor_type);
      },
      [=](register_query_atom, const string &description, const string &sensor_type, const string &strategy) {
        // rpc to register queries
        this->state.coordinatorPtr->register_query(description, sensor_type, strategy);
      },
      [=](delete_query_atom, const string &description) {
        // rpc to unregister a registered query
        this->delete_query(description);
      },
      [=](deploy_query_atom, const string &description) {
        // rpc to deploy queries to all corresponding actors
        this->deploy_query(description);
      },
      [=](execute_query_atom, const string &description, string &executableTransferObject) {
        // internal rpc to execute a query
        this->execute_query(description, executableTransferObject);
      },
      [=](get_operators_atom) {
        // internal rpc to return currently running operators on this node
        return this->state.coordinatorPtr->getOperators();
      },
      // external methods for users
      [=](topology_json_atom) {
        // print the topology
        string topo = this->state.coordinatorPtr->getTopologyPlanString();
        aout(this) << "Printing Topology" << endl;
        aout(this) << topo << endl;
      },
      [=](show_registered_atom) {
        // print registered queries
        aout(this) << "Printing Registered Queries" << endl;
        for (const auto &p : this->state.coordinatorPtr->getRegisteredQueries()) {
          aout(this) << p.first << endl;
        }
      },
      [=](show_running_atom) {
        // print running queries
        aout(this) << "Printing Running Queries" << endl;
        for (const auto &p : this->state.coordinatorPtr->getRunningQueries()) {
          aout(this) << p.first << endl;
        }
      },
      [=](show_running_operators_atom) {
        // print running operators in the whole topology
        aout(this) << "Requesting deployed operators from connected devices.." << endl;
        this->show_operators();
      }
  };
}

void actor_coordinator::register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                                        const string &sensor) {
  auto sap = current_sender();
  auto hdl = actor_cast<actor>(sap);
  FogTopologyEntryPtr
      sensorNode = this->state.coordinatorPtr->register_sensor(ip, publish_port, receive_port, cpu, sensor);

  this->state.actorTopologyMap.insert({sap, sensorNode});
  this->state.topologyActorMap.insert({sensorNode, sap});
  this->monitor(hdl);
  IOTDB_INFO("*** successfully registered sensor (CPU=" << cpu << ", Type: " << sensor << ") " << to_string(hdl));
}

void actor_coordinator::deploy_query(const string &description) {
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject>
      deployments = this->state.coordinatorPtr->make_deployment(description);

  for (auto const &x : deployments) {
    strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
    auto hdl = actor_cast<actor>(sap);
    string s_eto = SerializationTools::ser_eto(x.second);
    IOTDB_INFO("Sending query " << description << " to " << to_string(hdl));
    this->request(hdl, task_timeout, execute_query_atom::value, description, s_eto);
  }
}

/**
 * @brief framework internal method which is called to execute a query or sub-query on a node
 * @param description a description of the query
 * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
 */
void actor_coordinator::execute_query(const string &description, string &executableTransferObject) {
  ExecutableTransferObject eto = SerializationTools::parse_eto(executableTransferObject);
  QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
  this->state.runningQueries.insert({description, std::make_tuple(qep, eto.getOperatorTree())});
  this->state.enginePtr->deployQuery(qep);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

/**
 * @brief method which is called to unregister an already running query
 * @param description the description of the query
 */
void actor_coordinator::delete_query(const string &description) {
  // send command to all corresponding nodes to stop the running query as well
  for (auto const &x : this->state.actorTopologyMap) {
    strong_actor_ptr sap = x.first;
    FogTopologyEntryPtr e = x.second;

    if (e != this->state.coordinatorPtr->getThisEntry()) {
      auto hdl = actor_cast<actor>(sap);
      this->request(hdl, task_timeout, delete_query_atom::value, description);
    }
  }

  if (this->state.coordinatorPtr->deregister_query(description)) {
    //Query is running -> stop query locally if it is running and free resources
    try {
      this->state.enginePtr->undeployQuery(get<0>(this->state.runningQueries.at(description)));
      std::this_thread::sleep_for(std::chrono::seconds(3));
      this->state.runningQueries.erase(description);
    }
    catch (...) {
      // TODO: catch ZMQ termination errors properly
      IOTDB_ERROR("Uncaught error during deletion!")
    }
  }
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
        [=](const error &) {
          IOTDB_ERROR("Error for " << to_string(hdl));
        }
    );
  }
}

}