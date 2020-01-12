#include <Actors/CoordinatorActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Util/SerializationTools.hpp>
#include <Util/Logger.hpp>
namespace NES {

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
          aout(this) << "ACTORCOORDINATOR: Lost connection to worker " << key
                     << endl;
          key->get()->unregister_from_system();
        }
      });
  return running();
}

void CoordinatorActor::initializeNESTopology() {

  NESTopologyManager::getInstance().resetNESTopologyPlan();
  auto coordinatorNode = NESTopologyManager::getInstance()
      .createNESCoordinatorNode(actorCoordinatorConfig.ip, CPUCapacity::HIGH);
  coordinatorNode->setPublishPort(actorCoordinatorConfig.publish_port);
  coordinatorNode->setReceivePort(actorCoordinatorConfig.receive_port);
}

behavior CoordinatorActor::running() {
  return {
    // coordinator specific methods
    [=](register_sensor_atom, string& ip, uint16_t publish_port, uint16_t receive_port, int cpu,
        const string& nodeProperties, std::string sourceType, std::string sourceConf, std::string physicalStreamName, std::string logicalStreamName) {
      PhysicalStreamConfig streamConf(sourceType, sourceConf, physicalStreamName, logicalStreamName);
      this->registerSensor(ip, publish_port, receive_port, cpu, nodeProperties, streamConf);
    },
    [=](deregister_sensor_atom, string& ip) {
      IOTDB_DEBUG("CoordinatorActor: got request to remove node with ip " << ip)
      return this->deregisterSensor(ip);
    },
    [=](register_phy_stream_atom, std::string ip, std::string sourceType, std::string sourceConf,std::string physicalStreamName,std::string logicalStreamName) {
      PhysicalStreamConfig conf(sourceType, sourceConf, physicalStreamName, logicalStreamName);
      return registerPhysicalStream(ip, conf);
    },
    [=](register_log_stream_atom, const string& streamName, const string& streamSchema) {
      IOTDB_DEBUG("CoordinatorActor: got request for register logical stream " << streamName << " and schema " << streamSchema)
      SchemaPtr sch = UtilityFunctions::createSchemaFromCode(streamSchema);
      IOTDB_DEBUG("CoordinatorActor: schema successfully created")
      return registerLogicalStream(streamName, sch);
    },
    [=](remove_log_stream_atom, const string& streamName) {
      IOTDB_DEBUG("CoordinatorActor: got request for removel of logical stream " << streamName)
      return removeLogicalStream(streamName);
    },
    [=](remove_phy_stream_atom, std::string ip, const string& logicalStreamName, const string& physicalStreamName) {
      IOTDB_DEBUG("CoordinatorActor: got request for removel of physical stream " << physicalStreamName << " from logical stream " << logicalStreamName)
      PhysicalStreamConfig conf("", "", physicalStreamName, logicalStreamName);
      return removePhysicalStream(ip, conf);
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
    [=](show_registered_queries_atom) {
      aout(this) << "Printing Registered Queries" << endl;
      for (const auto& p : coordinatorServicePtr->getRegisteredQueries()) {
        aout(this) << p.first << endl;
      }
      return coordinatorServicePtr->getRegisteredQueries().size();
    },
    [=](show_running_queries_atom) {
      aout(this) << "Printing Running Queries" << endl;
      for (const auto& p : coordinatorServicePtr->getRunningQueries()) {
        aout(this) << p.first << endl;
      }
    },
    [=](show_reg_log_stream_atom) {
      aout(this) << "Printing logical streams" << endl;
      aout(this) << StreamCatalog::instance().getLogicalStreamAndSchemaAsString();
    },
    [=](show_reg_phy_stream_atom) {
      aout(this) << "Printing physical streams" << endl;
      aout(this) << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString();
    },
    [=](show_running_operators_atom) {
      aout(this) << "Requesting deployed operators from connected devices.." << endl;
      this->showOperators();
    }
  };
}

bool CoordinatorActor::removeLogicalStream(std::string logicalStreamName) {
  return StreamCatalog::instance().removeLogicalStream(logicalStreamName);
}

bool CoordinatorActor::registerLogicalStream(std::string logicalStreamName,
                                             SchemaPtr schemaPtr) {
  return StreamCatalog::instance().addLogicalStream(logicalStreamName,
                                                    schemaPtr);
}

bool CoordinatorActor::removePhysicalStream(std::string ip,
                                            PhysicalStreamConfig streamConf) {
  IOTDB_DEBUG(
      "CoordinatorActor: try to remove physical stream with ip " << ip << " physical name " << streamConf.physicalStreamName << " logical name " << streamConf.logicalStreamName)
  std::vector<NESTopologyEntryPtr> sensorNodes =
      NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeByIp(ip);

  //TODO: note that we remove on the first node with this id
  NESTopologyEntryPtr sensorNode;
  for (auto e : sensorNodes) {
    if (e->getEntryType() != Coordinator) {
      sensorNode = e;
      break;
    }
  }

  if (sensorNode == nullptr) {
    IOTDB_DEBUG("CoordinatorActor: sensor not found with ip " << ip)
    return false;
  }

  IOTDB_DEBUG("node type=" << sensorNode->getEntryTypeString())
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

  return StreamCatalog::instance().removePhysicalStream(
      streamConf.logicalStreamName, sce);
}

bool CoordinatorActor::registerPhysicalStream(std::string ip,
                                              PhysicalStreamConfig streamConf) {

  std::vector<NESTopologyEntryPtr> sensorNodes =
      NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeByIp(ip);

  //TODO: note that we register on the first node with this id
  NESTopologyEntryPtr sensorNode;
  for (auto e : sensorNodes) {
    if (e->getEntryType() != Coordinator) {
      sensorNode = e;
      break;
    }
  }

  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

  return StreamCatalog::instance().addPhysicalStream(
      streamConf.logicalStreamName, sce);
}

bool CoordinatorActor::deregisterSensor(const string &ip) {
  IOTDB_DEBUG("CoordinatorActor: try to disconnect sensor with ip " << ip)
  auto sap = current_sender();
  auto hdl = actor_cast<actor>(sap);

  std::vector<NESTopologyEntryPtr> sensorNodes =
      NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeByIp(ip);
  size_t cnt = 0;
  NESTopologyEntryPtr sensorNode;
  for (auto e : sensorNodes) {
    if (e->getEntryType() != Coordinator) {
      sensorNode = e;
      cnt++;
    }
  }
  if (cnt != 1) {
    IOTDB_ERROR(
        "CoordinatorActor: more than one worker node found with ip " << ip)
    throw Exception("node is ambitious");
  }
  IOTDB_DEBUG("CoordinatorActor: found sensor, try to delete it in catalog")
  //remove from catalog
  bool successCatalog = StreamCatalog::instance().removePhysicalStreamsByIp(ip);
  IOTDB_DEBUG("CoordinatorActor: success in catalog is " << successCatalog)

  IOTDB_DEBUG("CoordinatorActor: found sensor, try to delete it in toplogy")
  //remove from topology
  bool successTopology = coordinatorServicePtr->deregister_sensor(sensorNode);
  IOTDB_DEBUG("CoordinatorActor: success in topologyy is " << successTopology)

  if (successCatalog && successTopology)
    IOTDB_DEBUG("CoordinatorActor: sensor successfully removed")
  else
    IOTDB_ERROR("CoordinatorActor: sensor was not removed")

}

void CoordinatorActor::registerSensor(const string &ip, uint16_t publish_port,
                                      uint16_t receive_port, int cpu,
                                      const string &nodeProperties,
                                      PhysicalStreamConfig streamConf) {
  auto sap = current_sender();
  auto hdl = actor_cast<actor>(sap);
  NESTopologyEntryPtr sensorNode = coordinatorServicePtr->register_sensor(
      ip, publish_port, receive_port, cpu, nodeProperties, streamConf);

  this->state.actorTopologyMap.insert( { sap, sensorNode });
  this->state.topologyActorMap.insert( { sensorNode, sap });
  this->monitor(hdl);
  IOTDB_INFO(
      "ACTORCOORDINATOR: Successfully registered sensor (CPU=" << cpu << ", PhysicalStream: " << streamConf.physicalStreamName << ") " << to_string(hdl));
}

void CoordinatorActor::deployQuery(const string &queryId) {
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> deployments =
      coordinatorServicePtr->make_deployment(queryId);

  for (auto const &x : deployments) {
    strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
    auto hdl = actor_cast<actor>(sap);
    string s_eto = SerializationTools::ser_eto(x.second);
    IOTDB_INFO("Sending query " << queryId << " to " << to_string(hdl));
    this->request(hdl, task_timeout, execute_operators_atom::value, queryId,
                  s_eto);
  }
}

void CoordinatorActor::deregisterQuery(const string &queryId) {
  // send command to all corresponding nodes to stop the running query as well
  for (auto const &x : this->state.actorTopologyMap) {
    auto hdl = actor_cast<actor>(x.first);
    IOTDB_INFO(
        "ACTORCOORDINATOR: Sending deletion request " << queryId << " to " << to_string(hdl));
    this->request(hdl, task_timeout, delete_query_atom::value, queryId);
  }
  coordinatorServicePtr->deregister_query(queryId);
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
          IOTDB_ERROR(
              "ACTORCOORDINATOR: Error during showOperators for " << to_string(hdl) << "\n" << error_msg);
        });
  }
}

string CoordinatorActor::registerQuery(const string &queryString,
                                       const string &strategy) {
  return coordinatorServicePtr->register_query(queryString, strategy);
}

string CoordinatorActor::executeQuery(const string &queryString,
                                      const string &strategy) {
  string queryId = coordinatorServicePtr->register_query(queryString, strategy);
  deployQuery(queryId);
  return queryId;
}

}
