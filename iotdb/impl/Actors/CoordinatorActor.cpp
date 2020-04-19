#include <Actors/CoordinatorActor.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Util/SerializationTools.hpp>
#include <Util/Logger.hpp>
#include <functional>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace NES {
/**
     * @brief the constructor of the coordinator to initialize the default objects
     */
CoordinatorActor::CoordinatorActor(caf::actor_config& cfg, std::string ip)
    :
    stateful_actor(cfg), coordinatorIp(ip) {

    queryCatalogServicePtr = QueryCatalogService::getInstance();
    streamCatalogServicePtr = StreamCatalogService::getInstance();
    coordinatorServicePtr = CoordinatorService::getInstance();
}

size_t getIdFromHandle(caf::strong_actor_ptr curSender) {
    std::hash<std::string> hashFn;
    boost::uuids::basic_random_generator<boost::mt19937> gen;
    boost::uuids::uuid u = gen();
    string uidString = boost::uuids::to_string(u);
    string idWithoutStart = to_string(curSender) + ::to_string(time(0)) + uidString;
    return hashFn(idWithoutStart);
}

behavior CoordinatorActor::init() {
    NESTopologyManager::getInstance().resetNESTopologyPlan();
    NES_DEBUG(
        "CoordinatorActor::initializeNESTopology: set coordinatorIp = " << coordinatorIp)

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
              coordinatorServicePtr->deregisterSensor(
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
      NES_WARNING("CoordinatorActor => error thrown in error handler:" << to_string(err))
      if (err != exit_reason::user_shutdown) {
          NES_ERROR("CoordinatorActor error handle")
      } else {
          NES_DEBUG("CoordinatorActor error comes from stopping")
      }
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

bool CoordinatorActor::removePhysicalStream(size_t workerId, string logicalStreamName,
                                            string physicalStreamName) {
    NES_DEBUG(
        "CoordinatorActor: try to remove physical stream with name " << physicalStreamName << " logical name "
                                                                     << logicalStreamName << " workerId=" << workerId)

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            workerId);

    //TODO: note that we remove on the first node with this id
    NESTopologyEntryPtr sensorNode;
    for (auto e : sensorNodes) {
        if (e->getEntryType() != Coordinator) {
            sensorNode = e;
            break;
        }
    }

    if (sensorNode == nullptr) {
        NES_DEBUG("CoordinatorActor: sensor not found with workerId" << workerId)
        return false;
    }
    NES_DEBUG("node type=" << sensorNode->getEntryTypeString())

    return StreamCatalog::instance().removePhysicalStream(logicalStreamName,
                                                          physicalStreamName,
                                                          workerId);
}

bool CoordinatorActor::registerPhysicalStream(size_t workerId, PhysicalStreamConfig streamConf) {
    NES_DEBUG("CoordinatorActor: try to register physical stream with conf= " << streamConf.toString() << " for workerId=" << workerId)

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            workerId);

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

bool CoordinatorActor::deregisterNode(size_t workerId) {
    NES_DEBUG("CoordinatorActor: try to disconnect sensor with id " << workerId)

    std::vector<NESTopologyEntryPtr> sensorNodes =
        NESTopologyManager::getInstance().getNESTopologyPlan()->getNodeById(
            workerId);

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
            "CoordinatorActor: more than one worker node found with id " << workerId << " cnt=" << cnt)
        throw Exception("node is ambitious");
    } else if (cnt == 0) {
        NES_ERROR("CoordinatorActor: node with id not found " << workerId)
        throw Exception("node not found");
    }
    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in catalog")
    //remove from catalog
    bool successCatalog = StreamCatalog::instance().removePhysicalStreamByHashId(
        workerId);
    NES_DEBUG("CoordinatorActor: success in catalog is " << successCatalog)

    NES_DEBUG("CoordinatorActor: found sensor, try to delete it in toplogy")
    //remove from topology
    bool successTopology = coordinatorServicePtr->deregisterSensor(sensorNode);
    NES_DEBUG("CoordinatorActor: success in topologyy is " << successTopology)

    this->state.actorTopologyMap.erase(current_sender());
    this->state.topologyActorMap.erase(sensorNode);
    this->state.idToActorMap.erase(workerId);

    if (successCatalog && successTopology) {
        NES_DEBUG("CoordinatorActor: sensor successfully removed")
        return true;
    } else {
        NES_ERROR("CoordinatorActor: sensor was not removed")
        return false;
    }
}

size_t CoordinatorActor::registerNode(const string& ip, uint16_t publish_port,
                                    uint16_t receive_port, int cpu,
                                    const string& nodeProperties,
                                    PhysicalStreamConfig streamConf,
                                    NESNodeType type) {

    auto curSender = current_sender();
    auto handle = actor_cast<actor>(curSender);
    size_t hashId = getIdFromHandle(curSender);
    NES_DEBUG(
        "CoordinatorActor: connect attempt with id=" << curSender->id() << " handle=" << to_string(handle) << " hashID="
                                                     << hashId << " type=" << type)

    NESTopologyEntryPtr node = coordinatorServicePtr->registerNode(
        hashId, ip, publish_port, receive_port, cpu, nodeProperties, streamConf, type);

    if (!node) {
        NES_ERROR("CoordinatorActor::registerNode : node not not created")
        return 0;
    }

    this->state.actorTopologyMap.insert(std::make_pair(curSender, node));
    this->state.topologyActorMap.insert(std::make_pair(node, curSender));
    this->state.idToActorMap.insert(std::make_pair(hashId, curSender));

    this->monitor(handle);

    NES_DEBUG(
        "CoordinatorActor: Successfully registered sensor (CPU=" << cpu << ", PhysicalStream: "
                                                                 << streamConf.physicalStreamName << ") "
                                                                 << to_string(handle) << " type=" << type);

    NESTopologyManager::getInstance().printNESTopologyPlan();
    return hashId;
}

bool CoordinatorActor::shutdown() {
    bool success = coordinatorServicePtr->shutdown();
    if (!success) {
        NES_ERROR("CoordinatorActor::shutdown: error while shutdown coordinatorService")
        throw new Exception("Error while stopping CoordinatorActor::shutdown");
    }
    bool success2 = StreamCatalog::instance().reset();
    if (!success2) {
        NES_ERROR("CoordinatorActor::shutdown: error while reset StreamCatalog")
        throw new Exception("Error while stopping CoordinatorActor::shutdown");
    }

    QueryCatalog::instance().clearQueries();
    this->state.actorTopologyMap.clear();
    this->state.idToActorMap.clear();
    this->state.topologyActorMap.clear();

    return true;
}

bool CoordinatorActor::deployQuery(size_t workerId, const string& queryId) {
    NES_DEBUG("CoordinatorActor::deployQuery: workerId=" << workerId << " queryId=" << queryId)

    map<NESTopologyEntryPtr, ExecutableTransferObject> deployments =
        coordinatorServicePtr->prepareExecutableTransferObject(queryId);

    NES_DEBUG(
        "CoordinatorActor::deployQuery deploy " << deployments.size() << " objects")

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
    return true;
}

bool CoordinatorActor::deregisterQuery(size_t workerId, const string& queryId) {
    NES_DEBUG("CoordinatorActor::deregisterQuery: workerId=" << workerId << " queryId=" << queryId)

    // send command to all corresponding nodes to stop the running query as well
    for (auto const& x : this->state.actorTopologyMap) {
        auto handle = actor_cast<actor>(x.first);
        NES_INFO(
            "CoordinatorActor: Sending deletion request " << queryId << " to " << to_string(handle));
        this->request(handle, task_timeout, delete_query_atom::value, queryId);
    }
    return coordinatorServicePtr->deleteQuery(queryId);
}

string CoordinatorActor::registerQuery(size_t workerId, const string& queryString,
                                       const string& strategy) {
    NES_DEBUG("CoordinatorActor::registerQuery workerId=" << workerId << " queryString=" << queryString << " strategy=" << strategy)
    return coordinatorServicePtr->registerQuery(queryString, strategy);
}

string CoordinatorActor::executeQuery(size_t workerId, const string& queryString,
                                      const string& strategy) {
    NES_DEBUG("CoordinatorActor::executeQuery workerId=" << workerId << " queryString=" << queryString << " strategy=" << strategy)
    string queryId = coordinatorServicePtr->registerQuery(queryString, strategy);
    deployQuery(workerId, queryId);
    return queryId;
}

behavior CoordinatorActor::running() {
    NES_DEBUG("CoordinatorActor::running enter running")
    return {
        // coordinator specific methods
        [=](register_node_atom,
            string& ip,
            uint16_t publish_port,
            uint16_t receive_port,
            int cpu,
            const string& nodeProperties,
            int type) {
          PhysicalStreamConfig streamConf;
          size_t id = registerNode(ip, publish_port, receive_port, cpu, nodeProperties, streamConf, (NESNodeType) type);
          NES_DEBUG("CoordinatorActor::register_node_atom return id= " << id)
          return id;
        },
        [=](deregister_node_atom, size_t& id) {
          NES_DEBUG("CoordinatorActor: got request to remove node with ip " << id)
          return this->deregisterNode(id);
        },
        [=](register_phy_stream_atom,
            size_t id,
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
          return registerPhysicalStream(id, conf);
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

        [=](remove_phy_stream_atom, std::size_t id, const string& logicalStreamName,
            const string& physicalStreamName) {
          NES_DEBUG("CoordinatorActor: got request for removal of physical stream " << physicalStreamName
                                                                                    << " from logical stream "
                                                                                    << logicalStreamName)
          return removePhysicalStream(id, logicalStreamName, physicalStreamName);
        },
        [=](execute_query_atom, size_t id, const string& description, const string& strategy) {
          return executeQuery(id, description, strategy);
        },
        [=](register_query_atom, size_t id, const string& description, const string& strategy) {
          return registerQuery(id, description, strategy);
        },
        [=](deregister_query_atom, size_t id, const string& queryId) {
          return deregisterQuery(id, queryId);
        },
        [=](deploy_query_atom, size_t id, const string& description) {  //TODO:chef if we really need this except for testing
          return deployQuery(id, description);
        },
        [=](terminate_atom) {
          NES_DEBUG("CoordinatorActor::running(): terminate_atom")
          return shutdown();
        }
    };
    NES_DEBUG("CoordinatorActor::running end running")
}

}
