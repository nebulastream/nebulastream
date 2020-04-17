#include <Actors/WorkerActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Actors/AtomUtils.hpp>
#include <boost/filesystem.hpp>
#include <Util/Logger.hpp>
#include <sstream>


namespace NES {
WorkerActor::WorkerActor(actor_config& cfg, string ip, uint16_t publish_port,
                         uint16_t receive_port, NESNodeType type)
    :
    stateful_actor(cfg) {
    NES_DEBUG("")
    this->state.workerPtr = std::make_unique<WorkerService>(
        WorkerService(std::move(ip), publish_port, receive_port));
    this->type = type;
}

behavior WorkerActor::init() {
    // transition to `unconnected` on server failure
    this->set_down_handler([=](const down_msg& dm) {
      if (dm.source == this->state.current_server) {
          NES_WARNING("WorkerActor => lost connection to coordinator:" << to_string(dm))
          this->state.current_server = nullptr;
          this->become(unconnected());
      } else {
          NES_WARNING("WorkerActor => got message from another server:" << to_string(dm))
      }
    });

    this->set_exit_handler([=](const caf::exit_msg& em) {
      NES_DEBUG("WorkerActor => exit via handler:" << to_string(em))
    });

    this->set_error_handler([=](const caf::error& err) {
      NES_WARNING("WorkerActor => error thrown in error handler:" << to_string(err))
      NES_WARNING(" from " << this)
      if(err != exit_reason::user_shutdown)
      {
          NES_ERROR("WorkerActor error handle")
      }
      else
      {
          NES_DEBUG("WorkerActor error comes from stopping")
      }
    });

    this->set_exception_handler([](std::exception_ptr& err) -> error {
      try {
          std::rethrow_exception(err);
      }
      catch (const std::exception& e) {
          NES_ERROR("WorkerActor => error thrown in error handler:" << e.what())
          assert(0);
      }
      assert(0);
    });

    return unconnected();
}

behavior WorkerActor::unconnected() {
    NES_DEBUG("WorkerActor: become unconnected")
    return {
        [=](connect_atom, const std::string& host, uint16_t port) {
          NES_DEBUG("WorkerActor: unconnected() try reconnect to host=" << host << " port=" << port)
          connecting(host, port);
        },
        [=](terminate_atom) {
          NES_DEBUG("WorkerActor::running() shutdown")
          return shutdown();
        }
    };
}

bool WorkerActor::registerPhysicalStream(PhysicalStreamConfig conf) {
    NES_DEBUG("WorkerActor::registerPhysicalStream: got stream config=" << conf.toString())
    this->state.workerPtr->addPhysicalStreamConfig(conf);

    //send request to coordinator
    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<bool> prom;
    this->request(coordinator, task_timeout, register_phy_stream_atom::value,
                  conf.sourceType,
                  conf.sourceConfig, conf.sourceFrequency,
                  conf.numberOfBuffersToProduce, conf.physicalStreamName,
                  conf.logicalStreamName).await(
        [&prom, &conf](const bool& ret) mutable {
          if (ret == true) {
              NES_DEBUG(
                  "WorkerActor: registerPhysicalStream" << conf.physicalStreamName
                                                        << " successfully added")
          } else {
              NES_DEBUG(
                  "WorkerActor: registerPhysicalStream" << conf.physicalStreamName
                                                        << " could not be added")
          }
          prom.set_value(ret);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during registerLogicalStream for " << conf.physicalStreamName << "\n"
                                                                     << error_msg);
          throw new Exception("Error while registerPhysicalStream");
        }
    );
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::registerPhysicalStream: success=" << success)
    return success;
}

bool WorkerActor::registerLogicalStream(std::string streamName,
                                        std::string filePath) {
    NES_DEBUG(
        "WorkerActor: registerLogicalStream " << streamName << " with path" << filePath)

    auto coordinator = actor_cast<actor>(this->state.current_server);

    // Check if file can be found on system and read.
    boost::filesystem::path path{filePath.c_str()};
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
    std::promise<bool> prom;
    this->request(coordinator, task_timeout, register_log_stream_atom::value,
                  streamName, fileContent).await(
        [=, &prom](bool ret) {
          if (ret == true) {
              NES_DEBUG(
                  "WorkerActor: logical stream " << streamName << " successfully added")
          } else {
              NES_DEBUG(
                  "WorkerActor: logical stream " << streamName << " could not be added")
          }
          prom.set_value(ret);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during registerLogicalStream for " << to_string(coordinator) << "\n"
                                                                     << error_msg);
          throw new Exception("Error while registerLogicalStream");
        });

    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::registerLogicalStream: success=" << success)
    return success;
}

bool WorkerActor::removePhysicalStream(std::string logicalStreamName,
                                       std::string physicalStreamName) {
    NES_DEBUG(
        "WorkerActor: removePhysicalStream physical stream" << physicalStreamName << " from logical stream "
                                                            << logicalStreamName)
    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<bool> prom;
    this->request(coordinator, task_timeout, remove_phy_stream_atom::value,
                  state.workerPtr->getIp(), logicalStreamName, physicalStreamName)
        .await(
            [=, &prom](bool ret) {
              if (ret == true) {
                  NES_DEBUG(
                      "WorkerActor: physical stream " << physicalStreamName
                                                      << " successfully removed from "
                                                      << logicalStreamName)
              } else {
                  NES_DEBUG(
                      "WorkerActor: physical stream " << physicalStreamName
                                                      << " could not be removed from "
                                                      << logicalStreamName)
              }
              prom.set_value(ret);
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "WorkerActor: Error during removeLogicalStream for " << to_string(coordinator)
                                                                       << "\n" << error_msg);
              throw new Exception("Error while removePhysicalStream");
            });
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::removePhysicalStream: success=" << success)
    return success;
}

bool WorkerActor::removeLogicalStream(std::string streamName) {
    NES_DEBUG("WorkerActor: removeLogicalStream stream" << streamName)

    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<bool> prom;
    this->request(coordinator, task_timeout, remove_log_stream_atom::value,
                  streamName).await(
        [=, &prom](bool ret) {
          if (ret == true) {
              NES_DEBUG("WorkerActor: stream successfully removed")
          } else {
              NES_DEBUG("WorkerActor: stream not removed")
          }
          prom.set_value(ret);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during removeLogicalStream for " << to_string(coordinator) << "\n"
                                                                   << error_msg);
          throw new Exception("Error while removeLogicalStream");
        });
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::removeLogicalStream: success=" << success)
    return success;
}

bool WorkerActor::addNewParentToSensorNode(std::string childId, std::string parentId) {
    NES_DEBUG("WorkerActor: addNewParentToSensorNode parentId" << parentId << " childId=" << childId)
    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<bool> prom;
    this->request(coordinator, task_timeout, add_parent_atom::value,
                  childId, parentId).await(
        [=, &prom](bool ret) {
          if (ret == true) {
              NES_DEBUG("WorkerActor: parent successfully added")
          } else {
              NES_DEBUG("WorkerActor: parent not added")
          }
          prom.set_value(ret);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during addNewParentToSensorNode for " << to_string(coordinator) << "\n"
                                                                        << error_msg);
          throw new Exception("Error while addNewParentToSensorNode");
        });
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::addNewParentToSensorNode: success=" << success)
    return success;
}

std::string WorkerActor::getIdFromServer() {
    NES_DEBUG("WorkerActor: getIdFromServer")

    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<string> prom;
    this->request(coordinator, task_timeout, get_own_id::value).await(
        [=, &prom](const std::string& id) {
          NES_DEBUG("WorkerActor: get id successfully=" << id)
          prom.set_value(id);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during removeParentFromSensorNode for " << to_string(coordinator)
                                                                          << "\n"
                                                                          << error_msg);
          throw new Exception("Error while getIdFromServer");
        });
    string id = prom.get_future().get();
    NES_DEBUG("WorkerActor::getIdFromServer: id=" << id)
    return id;
}

bool WorkerActor::removeParentFromSensorNode(std::string childId, std::string parentId) {
    NES_DEBUG("WorkerActor: removeParentFromSensorNode parentId" << parentId << " childId=" << childId)

    auto coordinator = actor_cast<actor>(this->state.current_server);

    std::promise<bool> prom;
    this->request(coordinator, task_timeout, remove_parent_atom::value,
                  childId, parentId).await(
        [=, &prom](bool ret) {
          if (ret == true) {
              NES_DEBUG("WorkerActor: parent successfully removed")
          } else {
              NES_DEBUG("WorkerActor: parent not removed")
          }
          prom.set_value(ret);
        },
        [=](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during removeParentFromSensorNode for " << to_string(coordinator)
                                                                          << "\n"
                                                                          << error_msg);
          throw new Exception("Error while removeParentFromSensorNode");
        });
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::removeParentFromSensorNode: success=" << success)
    return success;
}

bool WorkerActor::disconnecting() {
    NES_DEBUG(
        "WorkerActor: try to disconnect with ip " << this->state.workerPtr->getIp())
    auto coordinator = actor_cast<actor>(this->state.current_server);
    std::promise<bool> prom;
    this->request(coordinator, task_timeout, deregister_node_atom::value,
                  this->state.workerPtr->getIp()).await(
        [&prom](const bool& c) mutable {
          NES_DEBUG("WorkerActor: disconnecting() successfully")
          prom.set_value(c);
        },
        [=, &prom](const error& er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "WorkerActor: Error during disconnecting " << "\n" << error_msg);
          throw new Exception("Error while disconnecting");
        });
    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::disconnecting: success=" << success)
    return success;
}

bool WorkerActor::registerNode(NESNodeType type) {
    if(type ==  NESNodeType::Sensor)
    {
        NES_DEBUG("WorkerActor::registerNode: try to register a sensor")
    }
    else if(type ==  NESNodeType::Worker)
    {
        NES_DEBUG("WorkerActor::registerNode: try to register a worker")
    }
    else
    {
        NES_ERROR("WorkerActor::registerNode node type not supported " << type)
        throw new Exception("WorkerActor::registerNode wrong node type");
    }

    std::promise<bool> prom;
    auto coordinator = actor_cast<actor>(this->state.current_server);
    this->request(coordinator, task_timeout, register_node_atom::value,
                  this->state.workerPtr->getIp(),
                  this->state.workerPtr->getPublishPort(),
                  this->state.workerPtr->getReceivePort(),
                  2,
                  this->state.workerPtr->getNodeProperties(),
                  (int)type)
        .await(
            [=, &prom](const bool& c) mutable {
              NES_DEBUG("WorkerActor::registerNode: node registered successfully")
              prom.set_value(c);
            }, [=, &prom](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "WorkerActor::registerNode:node registered not successfully created " << "\n"
                                                                                            << error_msg);
              throw new Exception("Error while registerNode");
            });;

    bool success = prom.get_future().get();
    NES_DEBUG("WorkerActor::registerNode: success=" << success)
    return success;
}

bool WorkerActor::connecting(const std::string& host, uint16_t port) {
    NES_DEBUG(
        "WorkerActor::connecting try to connect to host=" << host << " port=" << port)
    this->state.current_server = nullptr;

    auto exp_remote_actor = system().middleman().remote_actor(host, port);
    if (!!exp_remote_actor) {
        NES_DEBUG("WorkerActor::connecting: got coordinator handle successfully")
        this->state.current_server = caf::actor_cast<caf::strong_actor_ptr>(exp_remote_actor.value());
    } else {
        // remote actor not ready (not published) --> retry at some point
        NES_ERROR("WorkerActor::connecting handle error for host=" << host << " port=" << port << " error="
                                                                   << caf::to_string(exp_remote_actor.error()));
        throw new Exception("Error while casting coordinator handle");
    }

    NES_DEBUG("WorkerActor::connecting: register node of type=" << type)
    bool success = registerNode(type);
    if (!success) {
        throw new Exception("Error while register sensor");
    } else {
        NES_DEBUG("WorkerActor::connecting: register successful")
    }

    NES_DEBUG("WorkerActor::connecting: monitor handle")
    this->monitor(this->state.current_server);

    NES_DEBUG("WorkerActor::connecting: become running")
    this->become(running());

    NES_DEBUG("WorkerActor::connecting: return")
    return true;
}

bool WorkerActor::WorkerActor::shutdown() {
    NES_DEBUG("WorkerActor: shutdown");
    this->state.workerPtr->shutDown();
    return true;
}

/**
 * @brief method to execute the running state of a worker actor
 */
behavior WorkerActor::running() {
    NES_DEBUG("WorkerActor::running enter running")
    //Note this methods are send from the coordinator
    return {
        // internal rpc to execute a query
        [=](execute_operators_atom, const string& queryId, string& executableTransferObject) {
          NES_DEBUG("WorkerActor: got request for execute_operators_atom queryId=" << queryId << " eto="
                                                                                   << executableTransferObject)
          return this->state.workerPtr->executeQuery(queryId, executableTransferObject);
        },
        // internal rpc to unregister a query
        [=](delete_query_atom, const string& queryId) {
          NES_DEBUG("WorkerActor: got request for delete_query_atom queryId=" << queryId)
          this->state.workerPtr->deleteQuery(queryId);
        },
        // internal rpc to execute a query
        [=](get_operators_atom) {
          return this->state.workerPtr->getOperators();
        }
    };
    NES_DEBUG("WorkerActor::running end running")
}
}
