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
    this->state.actorTopologyMap.insert({this->address().get(), kRootNode});
    this->state.topologyActorMap.insert({kRootNode, this->address().get()});

    // transition to `unconnected` on server failure
    this->set_down_handler(
        [=](const down_msg& dm) {
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
      .createNESCoordinatorNode(0, actorCoordinatorConfig.ip, CPUCapacity::HIGH);
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
            const string& nodeProperties,
            std::string sourceType,
            std::string sourceConf,
            std::string physicalStreamName,
            std::string logicalStreamName) {
          PhysicalStreamConfig streamConf(sourceType, sourceConf, physicalStreamName, logicalStreamName);
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
            std::string physicalStreamName,
            std::string logicalStreamName) {
          PhysicalStreamConfig conf(sourceType, sourceConf, physicalStreamName, logicalStreamName);
          return registerPhysicalStream(ip, conf);
        },
        [=](register_log_stream_atom, const string& streamName, const string& streamSchema) {
          NES_DEBUG("CoordinatorActor: got request for register logical stream " << streamName << " and schema "
                                                                                 << streamSchema)
          return streamCatalogServicePtr->addNewLogicalStream(streamName, streamSchema);
        },
        [=](remove_log_stream_atom, const string& streamName) {
          NES_DEBUG("CoordinatorActor: got request for removel of logical stream " << streamName)
          return streamCatalogServicePtr->removeLogicalStream(streamName);
        },
        [=](remove_phy_stream_atom, std::string ip, const string& logicalStreamName, const string& physicalStreamName) {
          NES_DEBUG("CoordinatorActor: got request for removel of physical stream " << physicalStreamName
                                                                                    << " from logical stream "
                                                                                    << logicalStreamName)
          PhysicalStreamConfig conf("", "", physicalStreamName, logicalStreamName);
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
        [=](deploy_query_atom, const string& description) {
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
    };
}

bool CoordinatorActor::removePhysicalStream(std::string ip,
                                            PhysicalStreamConfig streamConf) {
    NES_DEBUG(
        "CoordinatorActor: try to remove physical stream with ip " << ip << " physical name "
                                                                   << streamConf.physicalStreamName << " logical name "
                                                                   << streamConf.logicalStreamName)
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
        NES_DEBUG("CoordinatorActor: sensor not found with ip " << ip)
        return false;
    }

    NES_DEBUG("node type=" << sensorNode->getEntryTypeString())
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

bool CoordinatorActor::deregisterSensor(const string& ip) {
    NES_DEBUG("CoordinatorActor: try to disconnect sensor with ip " << ip)
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
        NES_ERROR(
            "CoordinatorActor: more than one worker node found with ip " << ip)
        throw Exception("node is ambitious");
    }
    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in catalog")
    //remove from catalog
    bool successCatalog = StreamCatalog::instance().removePhysicalStreamsByIp(ip);
    NES_DEBUG("CoordinatorActor: success in catalog is " << successCatalog)

    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in toplogy")
    //remove from topology
    bool successTopology = coordinatorServicePtr->deregister_sensor(sensorNode);
    NES_DEBUG("CoordinatorActor: success in topologyy is " << successTopology)

    if (successCatalog && successTopology) {
        NES_DEBUG("CoordinatorActor: sensor successfully removed")
        return true;
    } else {
        NES_ERROR("CoordinatorActor: sensor was not removed")
        return false;
    }
}

void CoordinatorActor::registerSensor(const string& ip, uint16_t publish_port,
                                      uint16_t receive_port, int cpu,
                                      const string& nodeProperties,
                                      PhysicalStreamConfig streamConf) {
    auto sap = current_sender();
    auto hdl = actor_cast<actor>(sap);
    NESTopologyEntryPtr sensorNode = coordinatorServicePtr->register_sensor(
        sap->id(), ip, publish_port, receive_port, cpu, nodeProperties,
        streamConf);

    this->state.actorTopologyMap.insert({sap, sensorNode});
    this->state.topologyActorMap.insert({sensorNode, sap});
    this->monitor(hdl);
    NES_INFO(
        "ACTORCOORDINATOR: Successfully registered sensor (CPU=" << cpu << ", PhysicalStream: "
                                                                 << streamConf.physicalStreamName << ") "
                                                                 << to_string(hdl));
}

void CoordinatorActor::deployQuery(const string& queryId) {
    map<NESTopologyEntryPtr, ExecutableTransferObject> deployments =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);

    for (auto const& x : deployments) {
        strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
        auto hdl = actor_cast<actor>(sap);
        string s_eto = SerializationTools::ser_eto(x.second);
        NES_INFO("Sending query " << queryId << " to " << to_string(hdl));
        this->request(hdl, task_timeout, execute_operators_atom::value, queryId,
                      s_eto);
    }
    QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Running);
}

void CoordinatorActor::deregisterQuery(const string& queryId) {
    // send command to all corresponding nodes to stop the running query as well
    for (auto const& x : this->state.actorTopologyMap) {
        auto hdl = actor_cast<actor>(x.first);
        NES_INFO(
            "ACTORCOORDINATOR: Sending deletion request " << queryId << " to " << to_string(hdl));
        this->request(hdl, task_timeout, delete_query_atom::value, queryId);
    }
    coordinatorServicePtr->deleteQuery(queryId);
}

void CoordinatorActor::showOperators() {
    for (auto const& x : this->state.actorTopologyMap) {
        strong_actor_ptr sap = x.first;
        auto hdl = actor_cast<actor>(sap);

        this->request(hdl, task_timeout, get_operators_atom::value).then(
            [=](const vector<string>& vec) {
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

}
