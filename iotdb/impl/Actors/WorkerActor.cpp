#include <Actors/WorkerActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/AtomUtils.hpp>
#include <Util/SerializationTools.hpp>
#include <fstream>
#include <boost/filesystem.hpp>
#include <Util/Logger.hpp>
namespace NES {

WorkerActor::WorkerActor(actor_config &cfg, string ip, uint16_t publish_port,
                         uint16_t receive_port)
    :
    stateful_actor(cfg) {
  this->state.workerPtr = std::make_unique<WorkerService>(
      WorkerService(std::move(ip), publish_port, receive_port));
}

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

bool WorkerActor::registerPhysicalStream(std::string sourceType,
                                         std::string sourceConf,
                                         std::string physicalStreamName,
                                         std::string logicalStreamName) {

  PhysicalStreamConfig conf(sourceType, sourceConf, physicalStreamName,
                            logicalStreamName);
  //register physical stream with worker
  this->state.workerPtr->addPhysicalStreamConfig(conf);

  //send request to coordinator
  auto coordinator = actor_cast<actor>(this->state.current_server);

  bool sucess = false;
  scoped_actor self { this->system() };
  self->request(coordinator, task_timeout, register_phy_stream_atom::value,
                this->state.workerPtr->getIp(), sourceType, sourceConf,
                physicalStreamName, logicalStreamName).receive(
      [&sucess, &physicalStreamName](const bool &dc) mutable {
        if (dc == true) {
          NES_DEBUG(
              "WorkerActor: registerPhysicalStream" << physicalStreamName << " successfully added")
          sucess = true;
        } else {
          NES_DEBUG(
              "WorkerActor: registerPhysicalStream" << physicalStreamName << " could not be added")
          sucess = false;
        }
      }
      ,
      [=, &sucess](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "WorkerActor: Error during registerLogicalStream for " << physicalStreamName << "\n" << error_msg);
//        throw Exception("error while register stream");
        sucess = false;
      }
  );

  return sucess;
}

bool WorkerActor::registerLogicalStream(std::string streamName,
                                        std::string filePath) {
  //send request to coordinator
  auto coordinator = actor_cast<actor>(this->state.current_server);

  NES_DEBUG(
      "WorkerActor: registerLog stream " << streamName << " with path" << filePath)
  /* Check if file can be found on system and read. */
  boost::filesystem::path path { filePath.c_str() };
  if (!boost::filesystem::exists(path)
      || !boost::filesystem::is_regular_file(path)) {
    NES_ERROR("WorkerActor: file does not exits")
    throw Exception("files does not exist");
  }

  /* Read file from file system. */
  std::ifstream ifs(path.string().c_str());
  std::string fileContent((std::istreambuf_iterator<char>(ifs)),
                          (std::istreambuf_iterator<char>()));

  NES_DEBUG("WorkerActor: file content:" << fileContent)
  bool success = false;
  bool connected = false;
  scoped_actor self { this->system() };
  self->request(coordinator, task_timeout, register_log_stream_atom::value,
                streamName, fileContent).receive(
      [&](bool ret) {
        if (ret == true) {
          NES_DEBUG(
              "WorkerActor: logical stream " << streamName << " successfully added")
          success = true;
        } else {
          NES_DEBUG(
              "WorkerActor: logical stream " << streamName << " could not be added")
          success = false;
        }
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "WorkerActor: Error during registerLogicalStream for " << to_string(coordinator) << "\n" << error_msg);
        throw Exception("error while register stream");
      });
  return success;
}

void WorkerActor::removePhysicalStream(std::string logicalStreamName,
                                       std::string physicalStreamName) {
  //send request to coordinator
  auto coordinator = actor_cast<actor>(this->state.current_server);

  NES_DEBUG(
      "WorkerActor: removePhysicalStream physical stream" << physicalStreamName << " from logical stream " << logicalStreamName)
  scoped_actor self { this->system() };
  self->request(coordinator, task_timeout, remove_phy_stream_atom::value,
                state.workerPtr->getIp(), logicalStreamName, physicalStreamName)
      .receive(
      [&](bool ret) {
        if (ret == true) {
          NES_DEBUG(
              "WorkerActor: physical stream " << physicalStreamName << " successfully removed from " << logicalStreamName)
        } else {
          NES_DEBUG(
              "WorkerActor: physical stream " << physicalStreamName << " could not be removed from " << logicalStreamName)
        }
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "WorkerActor: Error during removeLogicalStream for " << to_string(coordinator) << "\n" << error_msg);
      });
}

void WorkerActor::removeLogicalStream(std::string streamName) {
  //send request to coordinator
  auto coordinator = actor_cast<actor>(this->state.current_server);

  NES_DEBUG("WorkerActor: removeLogicalStream stream" << streamName)
  scoped_actor self { this->system() };
  self->request(coordinator, task_timeout, remove_log_stream_atom::value,
                streamName).receive(
      [&](bool ret) {
        if (ret == true) {
          NES_DEBUG("WorkerActor: stream successfully removed")
        } else {
          NES_DEBUG("WorkerActor: stream not removed")
        }
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "WorkerActor: Error during removeLogicalStream for " << to_string(coordinator) << "\n" << error_msg);
      });
}

bool WorkerActor::disconnecting() {
  //TODO: add coorect behaviour if disconnect fails
  auto coordinator = actor_cast<actor>(this->state.current_server);
  NES_DEBUG(
      "WorkerActor: try to disconnect with ip " << this->state.workerPtr->getIp())
  bool disconnected = false;
  scoped_actor self { this->system() };
  self->request(coordinator, task_timeout, deregister_sensor_atom::value,
                this->state.workerPtr->getIp()).receive(
      [&disconnected](const bool &c) mutable {
        disconnected = c;
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      ,
      [=](const error &er) {
        string error_msg = to_string(er);
        NES_ERROR(
            "WorkerActor: Error during disconnecting " << "\n" << error_msg);
      });
  this->monitor(coordinator);
  this->become(unconnected());
  return disconnected;
}
/**
 * @brief the ongoing connection state in the TSM
 * if connection works go to running state, otherwise go to unconnected state
 */
bool WorkerActor::connecting(const std::string &host, uint16_t port) {
  // make sure we are not pointing to an old server
  this->state.current_server = nullptr;
  // use request().await() to suspend regular behavior until MM responded
  auto mm = this->system().middleman().actor_handle();
  scoped_actor self { this->system() };
  self->request(mm, infinite, connect_atom::value, host, port).receive(
      [=](const node_id&, strong_actor_ptr &serv,
          const std::set<std::string> &ifs) {
        if (!serv) {
          aout(this) << R"(*** no server found at ")" << host << R"(":)" << port
                     << endl;
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
        //TODO: add serializable shipping object
        cout << "send properties to server" << endl;
        //send default physical stream
        this->request(
            coordinator,
            task_timeout,
            register_sensor_atom::value,
            this->state.workerPtr->getIp(),
            this->state.workerPtr->getPublishPort(),
            this->state.workerPtr->getReceivePort(),
            2,
            this->state.workerPtr->getNodeProperties(),
            this->state.workerPtr->getPhysicalStreamConfig("default_physical")
                .sourceType,
            this->state.workerPtr->getPhysicalStreamConfig("default_physical")
                .sourceConfig,
            this->state.workerPtr->getPhysicalStreamConfig("default_physical")
                .physicalStreamName,
            this->state.workerPtr->getPhysicalStreamConfig("default_physical")
                .logicalStreamName);
        cout << "properties set successful, now changing state" << endl;
        this->monitor(coordinator);
        this->become(running(coordinator));
      }
      ,
      [=](const error &err) {
        aout(this) << R"(*** cannot connect to ")" << host << R"(":)" << port
                   << " => " << this->system().render(err) << endl;
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
      return connecting(host, port);
    },
    [=](disconnect_atom) {
      return disconnecting();
    },
    // register physical stream
    [=](register_phy_stream_atom, std::string sourceType, std::string sourceConf, std::string physicalStreamName, std::string logicalStreamName) {
      return registerPhysicalStream(sourceType, sourceConf, physicalStreamName, logicalStreamName);
    },
    // remove logical stream
    [=](register_log_stream_atom, std::string name, std::string path) {
      return registerLogicalStream(name, path);
    },
    // register logical stream
    [=](remove_log_stream_atom, std::string name) {
      removeLogicalStream(name);
    },        //remove physical stream
    [=](remove_phy_stream_atom, std::string logicalName, std::string physicalName) {
      removePhysicalStream(logicalName, physicalName);
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
