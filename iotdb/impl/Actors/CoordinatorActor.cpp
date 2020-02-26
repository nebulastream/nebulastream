#include <Actors/CoordinatorActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Util/SerializationTools.hpp>
#include <Util/Logger.hpp>
#include <functional>
namespace NES {

size_t getIdFromHandle(caf::strong_actor_ptr sap) {
  std::hash<std::string> hashFn;
  string idWithoutStart = to_string(sap).substr(to_string(sap).find("@") + 1);
  return hashFn(idWithoutStart);

}
behavior CoordinatorActor::init() {
  initializeNESTopology();
  StreamCatalog::instance();  //initialize catalog
  auto kRootNode = NESTopologyManager::getInstance().getRootNode();
  this->state.actorTopologyMap.insert( { this->address().get(), kRootNode });
  this->state.topologyActorMap.insert( { kRootNode, this->address().get() });

  // transition to `unconnected` on server failure
  this->set_down_handler(
      [=](const down_msg &dm) {
        strong_actor_ptr key = dm.source.get();
        auto hdl = actor_cast<actor>(key);
        if (this->state.actorTopologyMap.find(key)
            != this->state.actorTopologyMap.end()) {
          // remove disconnected worker from topology
          coordinatorServicePtr->deregister_sensor(
              this->state.actorTopologyMap.at(key));
          this->state.topologyActorMap.erase(
              this->state.actorTopologyMap.at(key));
          this->state.actorTopologyMap.erase(key);
          NES_DEBUG("CoordinatorActor: Lost connection to worker " << key->id())
          key->get()->unregister_from_system();
        }
      });
  return running();
}

CoordinatorActor::~CoordinatorActor() {
  NES_DEBUG("CoordinatorActor: destructed")
}

void CoordinatorActor::initializeNESTopology() {

  NESTopologyManager::getInstance().resetNESTopologyPlan();
  auto coordinatorNode = NESTopologyManager::getInstance()
      .createNESCoordinatorNode(0, actorCoordinatorConfig.ip,
                                CPUCapacity::HIGH);
  coordinatorNode->setPublishPort(actorCoordinatorConfig.publish_port);
  coordinatorNode->setReceivePort(actorCoordinatorConfig.receive_port);
}

behavior CoordinatorActor::running() {
  return {
    // coordinator specific methods
    [=](register_sensor_atom,
        string& ip,
        uint16_t publish_port,
        uint16_t receive_port,
        int cpu,
        const string& nodeProperties) {
      PhysicalStreamConfig streamConf;
      this->registerSensor(ip, publish_port, receive_port, cpu, nodeProperties, streamConf);
    },
    [=](deregister_sensor_atom, string& ip) {
      NES_DEBUG("CoordinatorActor: got request to remove node with ip " << ip)
      return this->deregisterSensor(ip);
    },
    [=](register_phy_stream_atom,
        std::string ip,
        std::string sourceType,
        std::string sourceConf,
        size_t sourceFrequency,
        size_t numberOfBuffersToProduce,
        std::string physicalStreamName,
        std::string logicalStreamName) {
      PhysicalStreamConfig conf(sourceType, sourceConf, sourceFrequency, numberOfBuffersToProduce, physicalStreamName, logicalStreamName);
      return registerPhysicalStream(ip, conf);
    },
    [=](register_log_stream_atom, const string& streamName, const string& streamSchema) {
      NES_DEBUG("CoordinatorActor: got request for register logical stream " << streamName << " and schema "
          << streamSchema)
      return streamCatalogServicePtr->addNewLogicalStream(streamName, streamSchema);
    },
    [=](remove_log_stream_atom, const string& streamName) {
      NES_DEBUG("CoordinatorActor: got request for removal of logical stream " << streamName)
      return streamCatalogServicePtr->removeLogicalStream(streamName);
    },
    [=](remove_phy_stream_atom, std::string ip, const string& logicalStreamName, const string& physicalStreamName) {
      NES_DEBUG("CoordinatorActor: got request for removal of physical stream " << physicalStreamName
          << " from logical stream "
          << logicalStreamName)
      PhysicalStreamConfig conf;
      conf.logicalStreamName = logicalStreamName;
      conf.physicalStreamName = physicalStreamName;
      return removePhysicalStream(ip, conf);
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
    [=](deploy_query_atom, const string& description) {//TODO:chef if we really need this except for testing
      deployQuery(description);
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
    [=](terminate_atom) {
      return shutdown();
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
      for(auto [key, value] : registeredQueries) {
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
      for(auto [key, value] : allLogicalStreams) {
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
  }
  ;
}

bool CoordinatorActor::removePhysicalStream(std::string ip,
                                            PhysicalStreamConfig streamConf) {
  NES_DEBUG(
      "CoordinatorActor: try to remove physical stream with ip " << ip << " physical name " << streamConf.physicalStreamName << " logical name " << streamConf.logicalStreamName)

  auto sap = current_sender();
  size_t hashId = getIdFromHandle(sap);
  NES_DEBUG(
      "CoordinatorActor: removePhysicalStream id=" << sap->id() << " sap=" << to_string(sap) << " hashID=" << hashId)

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
    NES_DEBUG("CoordinatorActor: sensor not found with ip " << ip)
    return false;
  }

  NES_DEBUG("node type=" << sensorNode->getEntryTypeString())
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                   sensorNode);

  return StreamCatalog::instance().removePhysicalStream(
      streamConf.logicalStreamName, sce);
}

bool CoordinatorActor::registerPhysicalStream(std::string ip,
                                              PhysicalStreamConfig streamConf) {
  NES_DEBUG("CoordinatorActor: try to register physical stream with ip " << ip)

  auto sap = current_sender();
  size_t hashId = getIdFromHandle(sap);
  NES_DEBUG(
      "CoordinatorActor: register pyhsical stream id=" << sap->id() << " sap=" << to_string(sap) << " hashID=" << hashId)

  std::vector<NESTopologyEntryPtr> sensorNodes =
      NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
          hashId);

  assert(sensorNodes.size() == 1);

  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf, sensorNodes[0]);

  return StreamCatalog::instance().addPhysicalStream(
      streamConf.logicalStreamName, sce);
}

bool CoordinatorActor::deregisterSensor(const string &ip) {
  NES_DEBUG("CoordinatorActor: try to disconnect sensor with ip " << ip)
  auto sap = current_sender();

  size_t hashId = getIdFromHandle(sap);
  NES_DEBUG(
      "CoordinatorActor: deregister node id=" << sap->id() << " sap=" << to_string(sap) << " hashID=" << hashId << " sap=" << to_string(sap))

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
  bool successCatalog = StreamCatalog::instance().removePhysicalStreamsByIp(ip);
  NES_DEBUG("CoordinatorActor: success in catalog is " << successCatalog)

  NES_DEBUG("CoordinatorActor: found sensor, try to delete it in toplogy")
  //remove from topology
  bool successTopology = coordinatorServicePtr->deregister_sensor(sensorNode);
  NES_DEBUG("CoordinatorActor: success in topologyy is " << successTopology)

  this->state.actorTopologyMap.erase(sap);
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

void CoordinatorActor::registerSensor(const string &ip, uint16_t publish_port,
                                      uint16_t receive_port, int cpu,
                                      const string &nodeProperties,
                                      PhysicalStreamConfig streamConf) {
  auto sap = current_sender();
  auto hdl = actor_cast<actor>(sap);
  size_t hashId = getIdFromHandle(sap);
  NES_DEBUG(
      "CoordinatorActor: connect attempt with id=" << sap->id() << " handle=" << to_string(hdl) << " hashID=" << hashId)
  //TODO: we should rewrite the topology to store actor handles instead of ids
  NESTopologyEntryPtr sensorNode = coordinatorServicePtr->register_sensor(
      hashId, ip, publish_port, receive_port, cpu, nodeProperties, streamConf);

  this->state.actorTopologyMap.insert(std::make_pair(sap, sensorNode));
  this->state.topologyActorMap.insert(std::make_pair(sensorNode, sap));
  this->state.idToActorMap.insert(std::make_pair(hashId, sap));

  this->monitor(hdl);
  NES_DEBUG(
      "CoordinatorActor: Successfully registered sensor (CPU=" << cpu << ", PhysicalStream: " << streamConf.physicalStreamName << ") " << to_string(hdl));
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

void CoordinatorActor::deployQuery(const string &queryId) {
  map<NESTopologyEntryPtr, ExecutableTransferObject> deployments =
      coordinatorServicePtr->prepareExecutableTransferObject(queryId);

  for (auto const &x : deployments) {
    strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
    auto hdl = actor_cast<actor>(sap);
    string s_eto = SerializationTools::ser_eto(x.second);
    NES_INFO(
        "CoordinatorActor:: Sending query " << queryId << " to " << to_string(hdl));
    this->request(hdl, task_timeout, execute_operators_atom::value, queryId,
                  s_eto);
  }
  QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Running);
}

void CoordinatorActor::deregisterQuery(const string &queryId) {
  // send command to all corresponding nodes to stop the running query as well
  for (auto const &x : this->state.actorTopologyMap) {
    auto hdl = actor_cast<actor>(x.first);
    NES_INFO(
        "ACTORCOORDINATOR: Sending deletion request " << queryId << " to " << to_string(hdl));
    this->request(hdl, task_timeout, delete_query_atom::value, queryId);
  }
  coordinatorServicePtr->deleteQuery(queryId);
}

void CoordinatorActor::showOperators() {
  for (auto const &x : this->state.actorTopologyMap) {
    strong_actor_ptr sap = x.first;
    auto hdl = actor_cast<actor>(sap);

    this->request(hdl, task_timeout, get_operators_atom::value).then(
        [=](const vector<string> &vec) {
          std::ostringstream ss;
          ss << x.second->getEntryTypeString() << "::" << to_string(hdl) << ":"
             << endl;

          aout(this) << ss.str() << vec << endl;
        }
        ,
        [=](const error &er) {
          string error_msg = to_string(er);
          NES_ERROR(
              "ACTORCOORDINATOR: Error during showOperators for " << to_string(hdl) << "\n" << error_msg);
        });
  }
}

string CoordinatorActor::registerQuery(const string &queryString,
                                       const string &strategy) {
  return coordinatorServicePtr->registerQuery(queryString, strategy);
}

string CoordinatorActor::executeQuery(const string &queryString,
                                      const string &strategy) {
  string queryId = coordinatorServicePtr->registerQuery(queryString, strategy);
  deployQuery(queryId);
  return queryId;
}

}
