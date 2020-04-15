#include <Actors/CoordinatorActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Util/SerializationTools.hpp>
#include <Util/Logger.hpp>
#include <functional>

namespace NES {

size_t getIdFromHandle(caf::strong_actor_ptr curSender) {
    std::hash<std::string> hashFn;
    string idWithoutStart = to_string(curSender).substr(to_string(curSender).find("@") + 1);
    return hashFn(idWithoutStart);

}

behavior CoordinatorActor::init() {
    initializeNESTopology();
    StreamCatalog::instance();  //initialize catalog
    auto kRootNode = NESTopologyManager::getInstance().getRootNode();
    this->state.actorTopologyMap.insert({this->address().get(), kRootNode});
    this->state.topologyActorMap.insert({kRootNode, this->address().get()});

    // transition to `unconnected` on server failure
    this->set_down_handler(
        [=](const down_msg& dm) {
          strong_actor_ptr key = dm.source.get();
          auto handle = actor_cast<actor>(key);
          if (this->state.actorTopologyMap.find(key)
              != this->state.actorTopologyMap.end()) {
              // remove disconnected worker from topology
              coordinatorServicePtr->deregister_sensor(
                  this->state.actorTopologyMap.at(key));
              this->state.topologyActorMap.erase(
                  this->state.actorTopologyMap.at(key));
              this->state.actorTopologyMap.erase(key);
              NES_DEBUG("CoordinatorActor => Lost connection to worker " << key->id())
              key->get()->unregister_from_system();
          }
        });

    this->set_exit_handler([=](const caf::exit_msg& em) {
      NES_DEBUG("CoordinatorActor => exit via handler:" << to_string(em))
    });

    this->set_error_handler([=](const caf::error& err) {
      NES_ERROR("CoordinatorActor => error thrown in error handler:" << to_string(err))
      assert(0);
    });

    this->set_exception_handler([](std::exception_ptr& err) -> error {
      try {
          std::rethrow_exception(err);
      }
      catch (const std::exception& e) {
          NES_ERROR("CoordinatorActor => error thrown in error handler:" << e.what())
          assert(0);
      }
      assert(0);
    });




    return running();
}

CoordinatorActor::~CoordinatorActor() {
    NES_DEBUG("CoordinatorActor: destructed")
}

void CoordinatorActor::initializeNESTopology() {

    NESTopologyManager::getInstance().resetNESTopologyPlan();
    NES_DEBUG(
        "CoordinatorActor::initializeNESTopology: set coordinatorIp = " << coordinatorIp)
    auto coordinatorNode = NESTopologyManager::getInstance()
        .createNESCoordinatorNode(0, coordinatorIp, CPUCapacity::HIGH);
    coordinatorNode->setPublishPort(actorCoordinatorConfig.publish_port);
    coordinatorNode->setReceivePort(actorCoordinatorConfig.receive_port);
}

bool CoordinatorActor::addNewParentToSensorNode(std::string childId, std::string parentId) {
    NES_DEBUG(
        "CoordinatorActor::addNewParentToSensorNode try to add new parent with id=" << parentId
                                                                                    << " to childId="
                                                                                    << childId)
    std::istringstream issChild(childId);
    size_t childIdSizeT;
    issChild >> childIdSizeT;

    std::istringstream issParent(parentId);
    size_t parentIdSizeT;
    issParent >> parentIdSizeT;

    bool success = coordinatorServicePtr->addNewParentToSensorNode(childIdSizeT,
                                                                   parentIdSizeT);
    return success;
}

bool CoordinatorActor::removeParentFromSensorNode(std::string childId, std::string parentId) {
    NES_DEBUG(
        "CoordinatorActor::removeParentFromSensorNode try to remove parent with id=" << parentId
                                                                                     << " to childId="
                                                                                     << childId)
    std::istringstream issChild(childId);
    size_t childIdSizeT;
    issChild >> childIdSizeT;

    std::istringstream issParent(parentId);
    size_t parentIdSizeT;
    issParent >> parentIdSizeT;

    bool success = coordinatorServicePtr->removeParentFromSensorNode(childIdSizeT,
                                                                     parentIdSizeT);
    return success;
}

string CoordinatorActor::getOwnId() {
    auto curSender = current_sender();
    size_t hashId = getIdFromHandle(curSender);

    NES_DEBUG(
        "CoordinatorActor: register physical stream id=" << curSender->id() << " curSender=" << to_string(curSender)
                                                         << " hashID="
                                                         << hashId)

    return ::to_string(hashId);
}

bool CoordinatorActor::removePhysicalStream(string logicalStreamName,
                                            string physicalStreamName) {
    NES_DEBUG(
        "CoordinatorActor: try to remove physical stream with name " << physicalStreamName << " logical name "
                                                                     << logicalStreamName)

    auto curSender = current_sender();
    size_t hashId = getIdFromHandle(curSender);
    NES_DEBUG(
        "CoordinatorActor: removePhysicalStream id=" << curSender->id() << " curSender=" << to_string(curSender)
                                                     << " hashID="
                                                     << hashId)

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            hashId);

    //TODO: note that we remove on the first node with this id
    NESTopologyEntryPtr sensorNode;
    for (auto e : sensorNodes) {
        if (e->getEntryType() != Coordinator) {
            sensorNode = e;
            break;
        }
    }

    if (sensorNode == nullptr) {
        NES_DEBUG("CoordinatorActor: sensor not found with hashId" << hashId)
        return false;
    }
    NES_DEBUG("node type=" << sensorNode->getEntryTypeString())

    return StreamCatalog::instance().removePhysicalStream(logicalStreamName,
                                                          physicalStreamName,
                                                          hashId);
}

bool CoordinatorActor::registerPhysicalStream(PhysicalStreamConfig streamConf) {
    NES_DEBUG("CoordinatorActor: try to register physical stream with conf= " << streamConf.toString())

    auto curSender = current_sender();
    size_t hashId = getIdFromHandle(curSender);
    NES_DEBUG(
        "CoordinatorActor: register physical stream id=" << curSender->id() << " curSender=" << to_string(curSender)
                                                         << " hashID="
                                                         << hashId)

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            hashId);

    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorActor::registerPhysicalStream node not found")
        return false;
    }
    if (sensorNodes.size() > 1) {
        NES_ERROR("CoordinatorActor::registerPhysicalStream more than one node found")
        return false;
    }

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
        streamConf, sensorNodes[0]);

    return StreamCatalog::instance().addPhysicalStream(
        streamConf.logicalStreamName, sce);
}

bool CoordinatorActor::deregisterSensor(const string& ip) {
    NES_DEBUG("CoordinatorActor: try to disconnect sensor with ip " << ip)
    auto curSender = current_sender();

    size_t hashId = getIdFromHandle(curSender);
    NES_DEBUG(
        "CoordinatorActor: deregister node id=" << curSender->id() << " curSender=" << to_string(curSender)
                                                << " hashID="
                                                << hashId
                                                << " curSender=" << to_string(curSender))

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            hashId);

    size_t cnt = 0;

    NESTopologyEntryPtr sensorNode;
    for (auto e : sensorNodes) {
        if (e->getEntryType() != Coordinator) {
            sensorNode = e;
            cnt++;
        }
    }
    if (cnt > 1) {
        NES_ERROR(
            "CoordinatorActor: more than one worker node found with id " << hashId << " cnt=" << cnt)
        throw Exception("node is ambitious");
    } else if (cnt == 0) {
        NES_ERROR("CoordinatorActor: node with id not found " << hashId)
        throw Exception("node not found");
    }
    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in catalog")
    //remove from catalog
    bool successCatalog = StreamCatalog::instance().removePhysicalStreamByHashId(
        hashId);
    NES_DEBUG("CoordinatorActor: success in catalog is " << successCatalog)

    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in toplogy")
    //remove from topology
    bool successTopology = coordinatorServicePtr->deregister_sensor(sensorNode);
    NES_DEBUG("CoordinatorActor: success in topologyy is " << successTopology)

    this->state.actorTopologyMap.erase(curSender);
    this->state.topologyActorMap.erase(sensorNode);
    this->state.idToActorMap.erase(hashId);

    if (successCatalog && successTopology) {
        NES_DEBUG("CoordinatorActor: sensor successfully removed")
        return true;
    } else {
        NES_ERROR("CoordinatorActor: sensor was not removed")
        return false;
    }
}

bool CoordinatorActor::registerSensor(const string& ip, uint16_t publish_port,
                                      uint16_t receive_port, int cpu,
                                      const string& nodeProperties,
                                      PhysicalStreamConfig streamConf) {
    auto curSender = current_sender();
    auto handle = actor_cast<actor>(curSender);
    size_t hashId = getIdFromHandle(curSender);
    NES_DEBUG(
        "CoordinatorActor: connect attempt with id=" << curSender->id() << " handle=" << to_string(handle) << " hashID="
                                                     << hashId)

    NESTopologyEntryPtr sensorNode = coordinatorServicePtr->register_sensor(
        hashId, ip, publish_port, receive_port, cpu, nodeProperties, streamConf);

    if (!sensorNode) {
        NES_ERROR("CoordinatorActor::registerSensor sensor not not created")
        return false;
    }
    this->state.actorTopologyMap.insert(std::make_pair(curSender, sensorNode));
    this->state.topologyActorMap.insert(std::make_pair(sensorNode, curSender));
    this->state.idToActorMap.insert(std::make_pair(hashId, curSender));

    this->monitor(handle);

    NES_DEBUG(
        "CoordinatorActor: Successfully registered sensor (CPU=" << cpu << ", PhysicalStream: "
                                                                 << streamConf.physicalStreamName << ") "
                                                                 << to_string(handle));

    NESTopologyManager::getInstance().printNESTopologyPlan();
    return true;
}

void CoordinatorActor::shutdown() {
    coordinatorServicePtr->shutdown();
    workerServicePtr->shutDown();
    StreamCatalog::instance().reset();
    QueryCatalog::instance().clearQueries();
    this->state.actorTopologyMap.clear();
    this->state.idToActorMap.clear();
    this->state.topologyActorMap.clear();
}

void CoordinatorActor::deployQuery(const string& queryId) {
    map<NESTopologyEntryPtr, ExecutableTransferObject> deployments =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);

    NES_DEBUG(
        "CoordinatorActor::deployQuery deploy " << deployments.size() << " objects")

    std::promise<bool> prom;

    for (auto const& x : deployments) {
        NES_DEBUG("CoordinatorActor::deployQuery serialize " << x.first << " id=" << x.first->getId() << " eto="
                                                             << x.second.toString())
        strong_actor_ptr curSender = this->state.topologyActorMap.at(x.first);
        auto handle = actor_cast<actor>(curSender);
        string s_eto = SerializationTools::ser_eto(x.second);
        NES_DEBUG("CoordinatorActor::deployQuery topology before send "
                      << NESTopologyManager::getInstance().getNESTopologyPlanString())
        NES_INFO(
            "CoordinatorActor:: Sending query " << queryId << " to " << to_string(handle))

        anon_send(handle, execute_operators_atom::value, queryId, s_eto);
        //TODO: currently deploy and execute are one phase such that we cannot wait
    }
    QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Running);
}

void CoordinatorActor::deregisterQuery(const string& queryId) {
    // send command to all corresponding nodes to stop the running query as well
    for (auto const& x : this->state.actorTopologyMap) {
        auto handle = actor_cast<actor>(x.first);
        NES_INFO(
            "CoordinatorActor: Sending deletion request " << queryId << " to " << to_string(handle));
        this->request(handle, task_timeout, delete_query_atom::value, queryId);
    }
    coordinatorServicePtr->deleteQuery(queryId);
}

void CoordinatorActor::showOperators() {
    for (auto const& x : this->state.actorTopologyMap) {
        strong_actor_ptr curSender = x.first;
        auto handle = actor_cast<actor>(curSender);

        this->request(handle, task_timeout, get_operators_atom::value).then(
            [=](const vector<string>& vec) {
              std::ostringstream ss;
              ss << x.second->getEntryTypeString() << "::" << to_string(handle) << ":"
                 << endl;

              aout(this) << ss.str() << vec << endl;
            },
            [=](const error& er) {
              string error_msg = to_string(er);
              NES_ERROR(
                  "CoordinatorActor: Error during showOperators for " << to_string(handle) << "\n"
                                                                      << error_msg);
            });
    }
}

string CoordinatorActor::registerQuery(const string& queryString,
                                       const string& strategy) {
    return coordinatorServicePtr->registerQuery(queryString, strategy);
}

string CoordinatorActor::executeQuery(const string& queryString,
                                      const string& strategy) {
    string queryId = coordinatorServicePtr->registerQuery(queryString, strategy);
    deployQuery(queryId);
    return queryId;
}

behavior CoordinatorActor::running() {
    NES_DEBUG("CoordinatorActor::running enter running")
    return {
        // coordinator specific methods
        [=](register_sensor_atom,
            string& ip,
            uint16_t publish_port,
            uint16_t receive_port,
            int cpu,
            const string& nodeProperties) {
          PhysicalStreamConfig streamConf;
          bool success = registerSensor(ip, publish_port, receive_port, cpu, nodeProperties, streamConf);
          NES_DEBUG("CoordinatorActor::register_sensor_atom return success= " << success)
          return success;
        },
        [=](deregister_sensor_atom, string& ip) {
          NES_DEBUG("CoordinatorActor: got request to remove node with ip " << ip)
          return this->deregisterSensor(ip);
        },
        [=](get_own_id) {
          NES_DEBUG("CoordinatorActor: got request to get own Id ")
          return this->getOwnId();
        },
        [=](register_phy_stream_atom,
            std::string sourceType,
            std::string sourceConf,
            size_t sourceFrequency,
            size_t numberOfBuffersToProduce,
            std::string physicalStreamName,
            std::string logicalStreamName) {
          PhysicalStreamConfig conf(sourceType,
                                    sourceConf,
                                    sourceFrequency,
                                    numberOfBuffersToProduce,
                                    physicalStreamName,
                                    logicalStreamName);
          return registerPhysicalStream(conf);
        },
        [=](register_log_stream_atom, const string& streamName, const string& streamSchema) {
          NES_DEBUG(
              "CoordinatorActor: got request for register logical stream " << streamName << " and schema "
                                                                           << streamSchema)
          return streamCatalogServicePtr->addNewLogicalStream(streamName, streamSchema);
        },
        [=](remove_log_stream_atom, const string& streamName) {
          NES_DEBUG("CoordinatorActor: got request for removal of logical stream " << streamName)
          return streamCatalogServicePtr->removeLogicalStream(streamName);
        },
        [=](add_parent_atom, const std::string& childId, const std::string& parentId) {
          NES_DEBUG("CoordinatorActor: got request for add parent " << parentId << " childId=" << childId)
          return addNewParentToSensorNode(childId, parentId);
        },
        [=](remove_parent_atom, const std::string& childId, const std::string& parentId) {
          NES_DEBUG("CoordinatorActor: got request for remove parent " << parentId << " childId=" << childId)
          return removeParentFromSensorNode(childId, parentId);
        },

        [=](remove_phy_stream_atom, std::string ip, const string& logicalStreamName,
            const string& physicalStreamName) {
          NES_DEBUG("CoordinatorActor: got request for removal of physical stream " << physicalStreamName
                                                                                    << " from logical stream "
                                                                                    << logicalStreamName)
          return removePhysicalStream(logicalStreamName, physicalStreamName);
        },
        [=](execute_query_atom, const string& description, const string& strategy) {
          return executeQuery(description, strategy);
        },
        [=](register_query_atom, const string& description, const string& strategy) {
          return registerQuery(description, strategy);
        },
        [=](deregister_query_atom, const string& queryId) {
          deregisterQuery(queryId);
        },
        [=](deploy_query_atom,
            const string& description) {  //TODO:chef if we really need this except for testing
          deployQuery(description);
        },
        //worker specific methods
        [=](execute_operators_atom, const string& description, string& executableTransferObject) {
          NES_DEBUG("CoordinatorActor::running(): execute_operators_atom, coordinator got query to execute")
          return workerServicePtr->executeQuery(description, executableTransferObject);
        },
        [=](delete_query_atom, const string& query) {
          workerServicePtr->deleteQuery(query);
        },
        [=](get_operators_atom) {
          return workerServicePtr->getOperators();
        },
        [=](terminate_atom) {
          NES_DEBUG("CoordinatorActor::running(): terminate_atom")
          shutdown();
        },
        // external methods for users
        [=](topology_json_atom) {
          string topo = coordinatorServicePtr->getTopologyPlanString();
          NES_DEBUG("CoordinatorActor: Printing Topology");
          NES_DEBUG(topo);

        },
        [=](show_registered_queries_atom) {
          NES_INFO("CoordinatorActor: Printing Registered Queries");
          const map registeredQueries = queryCatalogServicePtr->getAllRegisteredQueries();
          for (auto[key, value] : registeredQueries) {
              NES_INFO(key << ":" << value);
          }
          return registeredQueries.size();
        },
        [=](show_running_queries_atom) {
          NES_INFO("CoordinatorActor: Printing Running Queries");
          for (const auto& p : coordinatorServicePtr->getRunningQueries()) {
              NES_INFO(p.first);
          }
        },
        [=](show_reg_log_stream_atom) {
          NES_INFO("CoordinatorActor: Printing logical streams");
          auto allLogicalStreams = streamCatalogServicePtr->getAllLogicalStreamAsString();
          for (auto[key, value] : allLogicalStreams) {
              NES_INFO(key << ":" << value);
          }
        },
        [=](show_reg_phy_stream_atom) {
          NES_INFO("CoordinatorActor: Printing physical streams");
          NES_INFO(StreamCatalog::instance().getPhysicalStreamAndSchemaAsString());
        },
        [=](show_running_operators_atom) {
          NES_INFO("CoordinatorActor: Requesting deployed operators from connected devices..");
          this->showOperators();
        }
    };
    NES_DEBUG("CoordinatorActor::running end running")
}

}
