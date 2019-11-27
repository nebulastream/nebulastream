#include <Actors/ActorWorker.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/atom_utils.hpp>
#include <Util/SerializationTools.hpp>

namespace iotdb {
// starting point of our FSM
behavior actor_worker::init() {
  // transition to `unconnected` on server failure
  this->set_down_handler([=](const down_msg &dm) {
    if (dm.source == this->state.current_server) {
      aout(this) << "*** lost connection to server" << endl;
      this->state.current_server = nullptr;
      this->become(unconnected());
    }
  });
  return unconnected();
}

behavior actor_worker::unconnected() {
  return {
      [=](connect_atom, const std::string &host, uint16_t port) {
        connecting(host, port);
      }
  };
}

/**
 * @brief the ongoing connection state in the TSM
 * if connection works go to running state, otherwise go to unconnected state
 */
void actor_worker::connecting(const std::string &host, uint16_t port) {
  // make sure we are not pointing to an old server
  this->state.current_server = nullptr;
  // use request().await() to suspend regular behavior until MM responded
  auto mm = this->system().middleman().actor_handle();
  this->request(mm, infinite, connect_atom::value, host, port).await(
      [=](const node_id &, strong_actor_ptr &serv,
          const std::set<std::string> &ifs) {
        if (!serv) {
          aout(this) << R"(*** no server found at ")" << host << R"(":)"
                     << port << endl;
          return;
        }
        if (!ifs.empty()) {
          aout(this) << R"(*** typed actor found at ")" << host << R"(":)"
                     << port << ", but expected an untyped actor " << endl;
          return;
        }
        aout(this) << "*** successfully connected to server" << endl;
        this->state.current_server = serv;
        auto hdl = actor_cast<actor>(serv);
        this->monitor(hdl);
        this->become(running(hdl));
      },
      [=](const error &err) {
        aout(this) << R"(*** cannot connect to ")" << host << R"(":)"
                   << port << " => " << this->system().render(err) << endl;
        this->become(unconnected());
      }
  );
}

/**
 * @brief the running of the worker
 */
behavior actor_worker::running(const actor &coordinator) {
  auto this_actor_ptr = actor_cast<strong_actor_ptr>(this);
  //TODO: change me
  this->request(coordinator, task_timeout, register_sensor_atom::value, this->state.ip,
                this->state.publish_port, this->state.receive_port, 2, this->state.sensor_type,
                this_actor_ptr);
  //this->request(coordinator, task_timeout, register_worker_atom::value, 2, this_actor_ptr);

  return {
      // the connect RPC to connect with the coordinator
      [=](connect_atom, const std::string &host, uint16_t port) {
        connecting(host, port);
      },
      // internal rpc to execute a query
      [=](execute_query_atom, const string &description, string &executableTransferObject) {
        // internal rpc to execute a query
        this->execute_query(description, executableTransferObject);
      },
      // internal rpc to unregister a query
      [=](delete_query_atom, const string &query) {
        this->delete_query(query);
      },
      // internal rpc to execute a query
      [=](get_operators_atom) {
        return this->getOperators();
      }
  };
}

/**
 * @brief framework internal method which is called to execute a query or sub-query on a node
 * @param description a description of the query
 * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
 */
void actor_worker::execute_query(const string &description, string &executableTransferObject) {
  ExecutableTransferObject eto = SerializationTools::parse_eto(executableTransferObject);
  QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
  this->state.runningQueries.insert({description, std::make_tuple(qep, eto.getOperatorTree())});
  this->state.enginePtr->deployQuery(qep);
  std::this_thread::sleep_for(std::chrono::seconds(2));
}

/**
* @brief method which is called to unregister an already running query
* @param query the description of the query
*/
void actor_worker::delete_query(const string &query) {
  auto sap = actor_cast<strong_actor_ptr>(this);
  try {
    if (this->state.runningQueries.find(query) != this->state.runningQueries.end()) {
      QueryExecutionPlanPtr qep = get<0>(this->state.runningQueries.at(query));
      this->state.enginePtr->undeployQuery(qep);
      std::this_thread::sleep_for(std::chrono::seconds(3));
      this->state.runningQueries.erase(query);
      aout(this) << to_string(sap) << ": *** Successfully deleted query " << query << endl;
    } else {
      aout(this) << to_string(sap) << ": *** Query not found for deletion -> " << query << endl;
    }
  }
  catch (...) {
    // TODO: catch ZMQ termination errors properly
    IOTDB_ERROR("Uncaugth error during deletion!")
  }
}

/**
 * @brief gets the currently locally running operators and returns them as flattened strings in a vector
 * @return the flattend vector<string> object of operators
 */
vector<string> actor_worker::getOperators() {
  vector<string> result;
  for (auto const &x : this->state.runningQueries) {
    string str_opts;
    std::set<OperatorType> flattened = get<1>(x.second)->flattenedTypes();
    for (const OperatorType &_o: flattened) {
      if (!str_opts.empty())
        str_opts.append(", ");
      str_opts.append(operatorTypeToString.at(_o));
    }
    result.emplace_back(x.first + "->" + str_opts);
  }
  return result;
}
}