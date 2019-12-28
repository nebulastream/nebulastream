#include <Actors/WorkerActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/AtomUtils.hpp>
#include <Util/SerializationTools.hpp>

namespace iotdb {
// starting point of our FSM
behavior WorkerActor::init() {
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

behavior WorkerActor::unconnected() {
  return {
    [=](connect_atom, const std::string &host, uint16_t port) {
      connecting(host, port);
      return true;
    }
  };
}

/**
 * @brief the ongoing connection state in the TSM
 * if connection works go to running state, otherwise go to unconnected state
 */
void WorkerActor::connecting(const std::string &host, uint16_t port) {
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
        auto coordinator = actor_cast<actor>(serv);
        //TODO: make getPhysicalStreamConfig serializable with the caf framework
        cout << "send properties to server" << endl;
        this->request(coordinator, task_timeout, register_sensor_atom::value, this->state.workerPtr->getIp(),
            this->state.workerPtr->getPublishPort(), this->state.workerPtr->getReceivePort(), 2,
            this->state.workerPtr->getNodeProperties(),
            this->state.workerPtr->getPhysicalStreamConfig().filePath,
            this->state.workerPtr->getPhysicalStreamConfig().physicalStreamName,
            this->state.workerPtr->getPhysicalStreamConfig().logicalStreamName
        );
        cout << "properties set successful, now changing state" << endl;
        this->monitor(coordinator);
        this->become(running(coordinator));
      },
      [=](const error &err) {
        aout(this) << R"(*** cannot connect to ")" << host << R"(":)"
        << port << " => " << this->system().render(err) << endl;
        this->become(unconnected());
      });
}

/**
 * @brief the running of the worker
 */
behavior WorkerActor::running(const actor &coordinator) {
  auto this_actor_ptr = actor_cast<strong_actor_ptr>(this);

  //waiting for incoming messages
  return {
    // the connect RPC to connect with the coordinator
    [=](connect_atom, const std::string &host, uint16_t port) {
      connecting(host, port);
    },
    // internal rpc to execute a query
    [=](execute_operators_atom, const string &queryId, string &executableTransferObject) {
      // internal rpc to execute a query
      this->state.workerPtr->execute_query(queryId, executableTransferObject);
    },
    // internal rpc to unregister a query
    [=](delete_query_atom, const string &query) {
      this->state.workerPtr->delete_query(query);
    },
    // internal rpc to execute a query
    [=](get_operators_atom) {
      return this->state.workerPtr->getOperators();
    }
  };
}
}
