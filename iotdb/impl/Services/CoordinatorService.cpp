#include <Actors/ExecutableTransferObject.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <Services/CoordinatorService.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace NES;
using namespace std;

string CoordinatorService::getNodePropertiesAsString(const NESTopologyEntryPtr entry) {
    return entry->getNodeProperty();
}

bool CoordinatorService::addNewParentToSensorNode(size_t childId, size_t parentId) {
    NESTopologyManager& topologyManager = this->topologyManagerPtr->getInstance();

    if (childId == parentId) {
        NES_ERROR("CoordinatorService::change_sensor_position: cannot add link to itself");
        return false;
    }

    std::vector<NESTopologyEntryPtr> sensorNodes = topologyManager.getNESTopologyPlan()->getNodeById(childId);
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: source node " << childId << " does not exists");
        return false;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorService::change_sensor_position: more than one sensorNodes node with id exists " << childId
                                                                                                         << " count="
                                                                                                         << sensorNodes.size());
        return false;
    }
    NES_ERROR("CoordinatorService::change_sensor_position: source node " << childId << " exists");

    std::vector<NESTopologyEntryPtr> sensorParent = topologyManager.getNESTopologyPlan()->getNodeById(parentId);
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: sensorParent node " << parentId << " does not exists");
        return false;
    } else if (sensorParent.size() > 1) {
        NES_ERROR("CoordinatorService::change_sensor_position: more than one sensorParent node with id exists "
                  << parentId << " count=" << sensorParent.size());
        return false;
    }
    NES_ERROR("CoordinatorService::change_sensor_position: sensorParent node " << parentId << " exists");

    bool connected = topologyManager.getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                          sensorParent[0]);
    if (connected) {
        NES_ERROR("CoordinatorService::change_sensor_position: nodes " << childId << " and " << parentId
                                                                       << " already exists");
        return false;
    }

    NESTopologyLinkPtr link = topologyManager.getNESTopologyPlan()->createNESTopologyLink(sensorNodes[0],
                                                                                          sensorParent[0], 1, 1);
    if (link) {
        NES_DEBUG("CoordinatorService::change_sensor_position: created link successfully");
    } else {
        NES_ERROR("CoordinatorService::change_sensor_position: created NOT successfully added");
    }

    return true;
}

bool CoordinatorService::removeParentFromSensorNode(size_t childId, size_t parentId) {
    NESTopologyManager& topologyManager = this->topologyManagerPtr->getInstance();
    std::vector<NESTopologyEntryPtr> sensorNodes = topologyManager.getNESTopologyPlan()->getNodeById(childId);
    if (sensorNodes.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: source node " << childId << " does not exists");
        return false;
    } else if (sensorNodes.size() > 1) {
        NES_ERROR(
            "CoordinatorService::change_sensor_position: more than one sensorNodes node with id exists " << childId
                                                                                                         << " count="
                                                                                                         << sensorNodes.size());
        return false;
    }

    std::vector<NESTopologyEntryPtr> sensorParent = topologyManager.getNESTopologyPlan()->getNodeById(parentId);
    if (sensorParent.size() == 0) {
        NES_ERROR("CoordinatorService::change_sensor_position: sensorParent node " << childId << " does not exists");
        return false;
    } else if (sensorParent.size() > 1) {
        NES_ERROR(
            "CoordinatorService::change_sensor_position: more than one sensorParent node with id exists " << childId
                                                                                                          << " count="
                                                                                                          << sensorNodes.size());
        return false;
    }

    bool connected = topologyManager.getNESTopologyPlan()->getNESTopologyGraph()->hasLink(sensorNodes[0],
                                                                                          sensorParent[0]);
    if (!connected) {
        NES_ERROR("CoordinatorService::change_sensor_position: nodes " << childId << " and " << parentId
                                                                       << " are not connected");
        return false;
    }

    NESTopologyLinkPtr link = topologyManager.getNESTopologyPlan()->getNESTopologyGraph()->getLink(sensorNodes[0],
                                                                                                   sensorParent[0]);

    bool success = topologyManager.getNESTopologyPlan()->getNESTopologyGraph()->removeEdge(link->getId());
    if (!success) {
        NES_ERROR("CoordinatorService::change_sensor_position: edge between  " << childId << " and " << parentId
                                                                               << " could not be removed");
        return false;
    }
    return true;
}

NESTopologyEntryPtr CoordinatorService::registerNode(size_t id, std::string address, int cpu, const string& nodeProperties,
                                                     PhysicalStreamConfig streamConf, NESNodeType type) {
    NESTopologyManager& topologyManager = this->topologyManagerPtr->getInstance();
    NESTopologyEntryPtr nodePtr;
    if (type == NESNodeType::Sensor) {
        NES_DEBUG("CoordinatorService::registerNode: register sensor node");
        nodePtr = topologyManager.createNESSensorNode(id, address, CPUCapacity::Value(cpu));
        NESTopologySensorNode* sensor = dynamic_cast<NESTopologySensorNode*>(nodePtr.get());
        sensor->setPhysicalStreamName(streamConf.physicalStreamName);

        NES_DEBUG(
            "try to register sensor phyName=" << streamConf.physicalStreamName << " logName="
                                              << streamConf.logicalStreamName << " nodeID=" << nodePtr->getId());

        //check if logical stream exists
        if (!StreamCatalog::instance().testIfLogicalStreamExistsInSchemaMapping(
                streamConf.logicalStreamName)) {
            NES_ERROR(
                "Coordinator: error logical stream" << streamConf.logicalStreamName
                                                    << " does not exist when adding physical stream "
                                                    << streamConf.physicalStreamName);
            throw Exception(
                "logical stream does not exist " + streamConf.logicalStreamName);
        }

        SchemaPtr schema = StreamCatalog::instance().getSchemaForLogicalStream(
            streamConf.logicalStreamName);

        DataSourcePtr source;
        if (streamConf.sourceType != "CSVSource"
            && streamConf.sourceType != "DefaultSource") {
            NES_ERROR(
                "Coordinator: error source type " << streamConf.sourceType << " is not supported");
            throw Exception(
                "Coordinator: error source type " + streamConf.sourceType
                + " is not supported");
        }

        StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
            streamConf, nodePtr);

        bool success = StreamCatalog::instance().addPhysicalStream(
            streamConf.logicalStreamName, sce);
        if (!success) {
            NES_ERROR(
                "Coordinator: physical stream " << streamConf.physicalStreamName << " could not be added to catalog");
            throw Exception(
                "Coordinator: physical stream " + streamConf.physicalStreamName
                + " could not be added to catalog");
        }

    } else if (type == NESNodeType::Worker) {
        NES_DEBUG("CoordinatorService::registerNode: register worker node");
        nodePtr = topologyManager.createNESWorkerNode(id, address, CPUCapacity::Value(cpu));
    } else {
        NES_ERROR("CoordinatorService::registerNode: type not supported " << type);
        assert(0);
    }
    assert(nodePtr);

//    nodePtr->setPublishPort(publish_port);
//    nodePtr->setReceivePort(receive_port);
    if (nodeProperties != "defaultProperties") {
        nodePtr->setNodeProperty(nodeProperties);
    }

    const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
                                              .getRootNode();

    if (kRootNode == nodePtr) {
        NES_DEBUG("CoordinatorService::registerNode: tree is empty so this becomes new root");
    } else {
        NES_DEBUG("CoordinatorService::registerNode: add link to root node " << kRootNode << " of type" << kRootNode->getEntryType());
        topologyManager.createNESTopologyLink(nodePtr, kRootNode, 1, 1);
    }

    return nodePtr;
}

string CoordinatorService::registerQuery(const string& queryString, const string& optimizationStrategyName) {
    return QueryCatalog::instance().registerQuery(queryString, optimizationStrategyName);
}

bool CoordinatorService::deleteQuery(const string& queryId) {
    return QueryCatalog::instance().deleteQuery(queryId);
}

bool CoordinatorService::shutdown() {
    queryToPort.clear();
    return true;
}

map<NESTopologyEntryPtr, ExecutableTransferObject> CoordinatorService::prepareExecutableTransferObject(
    const string& queryId) {
    map<NESTopologyEntryPtr, ExecutableTransferObject> output;
    if (QueryCatalog::instance().queryExists(queryId)
        && !QueryCatalog::instance().isQueryRunning(queryId)) {
        NES_INFO("CoordinatorService: prepareExecutableTransferObject for query " << queryId);

        NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)->getNesPlanPtr();

        SchemaPtr schema = QueryCatalog::instance().getQuery(queryId)->getInputQueryPtr()->getSourceStream()->getSchema();

        //iterate through all vertices in the topology
        for (const ExecutionVertex& v : execPlan->getExecutionGraph()->getAllVertex()) {
            OperatorPtr operators = v.ptr->getRootOperator();
            if (operators) {
                // if node contains operators to be deployed -> serialize and send them to the according node
                vector<DataSourcePtr> sources = getSources(queryId, v);
                vector<DataSinkPtr> destinations = getSinks(queryId, v);
                NESTopologyEntryPtr nesNode = v.ptr->getNESNode();
                //TODO: this will break for multiple streams
                ExecutableTransferObject eto = ExecutableTransferObject(queryId, schema,
                                                                        sources,
                                                                        destinations,
                                                                        operators);
                output.insert({nesNode, eto});
            }
        }

        QueryCatalog::instance().markQueryAs(queryId, QueryStatus::Scheduling);

    } else if (QueryCatalog::instance().getQuery(queryId)->getQueryStatus() == QueryStatus::Running) {
        NES_WARNING("CoordinatorService: Query is already running -> " << queryId);
    } else {
        NES_WARNING("CoordinatorService: Query is not registered -> " << queryId);
    }

    NES_INFO("CoordinatorService: prepareExecutableTransferObject successfully " << queryId);

    return output;
}

vector<DataSourcePtr> CoordinatorService::getSources(const string& queryId,
                                                     const ExecutionVertex& v) {
    vector<DataSourcePtr> out = vector<DataSourcePtr>();
    NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)->getNesPlanPtr();
    SchemaPtr schema = QueryCatalog::instance().getQuery(queryId)->getInputQueryPtr()->getSourceStream()->getSchema();

    DataSourcePtr source = findDataSourcePointer(v.ptr->getRootOperator());

    //FIXME: what about user defined a ZMQ sink?
    if (source->getType() == ZMQ_SOURCE) {
        //FIXME: Maybe a better way to do it? perhaps type cast to ZMQSource type and just update the port number
        // create local zmq source
        const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
                                                  .getRootNode();

        //TODO: this does not work this way
        BufferManagerPtr bPtr;
        QueryManagerPtr disPtr;
        source = createZmqSource(schema, bPtr, disPtr, kRootNode->getIp(), assign_port(queryId));
        //        assert(0);
    }
    out.emplace_back(source);
    return out;
}

vector<DataSinkPtr> CoordinatorService::getSinks(const string& queryId,
                                                 const ExecutionVertex& v) {
    vector<DataSinkPtr> out = vector<DataSinkPtr>();
    //TODO: I am not sure what is happening here with the idx 0 and 1
    NESExecutionPlanPtr execPlan = QueryCatalog::instance().getQuery(queryId)->getNesPlanPtr();
    DataSinkPtr sink = findDataSinkPointer(v.ptr->getRootOperator());

    //FIXME: what about user defined a ZMQ sink?
    if (sink->getType() == ZMQ_SINK) {
        //FIXME: Maybe a better way to do it? perhaps type cast to ZMQSink type and just update the port number
        //create local zmq sink
        const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
                                                  .getRootNode();

        sink = createZmqSink(sink->getSchema(), kRootNode->getIp(),
                             assign_port(queryId));
        NES_DEBUG("CoordinatorService::getSinks: " << sink->toString() << " iP=" << kRootNode->getIp());
    }
    out.emplace_back(sink);
    return out;
}

DataSinkPtr CoordinatorService::findDataSinkPointer(OperatorPtr operatorPtr) {

    if (!operatorPtr->getParent() && operatorPtr->getOperatorType() == SINK_OP) {
        SinkOperator* sinkOperator = dynamic_cast<SinkOperator*>(operatorPtr.get());
        return sinkOperator->getDataSinkPtr();
    } else if (operatorPtr->getParent() != nullptr) {
        return findDataSinkPointer(operatorPtr->getParent());
    } else {
        NES_WARNING("Found query graph without a SINK.");
        return nullptr;
    }
}

DataSourcePtr CoordinatorService::findDataSourcePointer(
    OperatorPtr operatorPtr) {
    vector<OperatorPtr> children = operatorPtr->getChildren();
    if (children.empty() && operatorPtr->getOperatorType() == SOURCE_OP) {
        SourceOperator* sourceOperator = dynamic_cast<SourceOperator*>(operatorPtr
                                                                           .get());
        return sourceOperator->getDataSourcePtr();
    } else if (!children.empty()) {
        //FIXME: What if there are more than one source?
        for (OperatorPtr child : children) {
            return findDataSourcePointer(operatorPtr->getParent());
        }
    }
    NES_WARNING("Found query graph without a SOURCE.");
    return nullptr;
}

int CoordinatorService::assign_port(const string& queryId) {
    if (this->queryToPort.find(queryId) != this->queryToPort.end()) {
        NES_DEBUG("CoordinatorService::assign_port query found with id=" << queryId << " return port=" << this->queryToPort.at(queryId));
        return this->queryToPort.at(queryId);
    } else {
        // increase max port in map by 1
        const NESTopologyEntryPtr kRootNode = NESTopologyManager::getInstance()
                                                  .getRootNode();
        uint16_t kFreeZmqPort = kRootNode->getNextFreeReceivePort();
        this->queryToPort.insert({queryId, kFreeZmqPort});
        NES_DEBUG("CoordinatorService::assign_port create a new port for query id=" << queryId << " port=" << kFreeZmqPort);
        return kFreeZmqPort;
    }
}

bool CoordinatorService::deregisterSensor(const NESTopologyEntryPtr entry) {
    NES_DEBUG("CoordinatorService::deregisterNode with id=" << entry->getId());
    return this->topologyManagerPtr->getInstance().removeNESNode(entry);
}

string CoordinatorService::getTopologyPlanString() {
    return this->topologyManagerPtr->getInstance().getNESTopologyPlanString();
}

NESExecutionPlanPtr CoordinatorService::getRegisteredQuery(string queryId) {
    if (QueryCatalog::instance().queryExists(queryId)) {
        NES_DEBUG("CoordinatorService: return existing query " << queryId);
        return QueryCatalog::instance().getQuery(queryId)->getNesPlanPtr();
    }
    NES_DEBUG("CoordinatorService: query with id does not exits" << queryId);
    return nullptr;
}

bool CoordinatorService::clearQueryCatalogs() {
    try {
        QueryCatalog::instance().clearQueries();
    } catch (...) {
        assert(0);
        return false;
    }
    return true;
}

const map<string, QueryCatalogEntryPtr> CoordinatorService::getRegisteredQueries() {
    return QueryCatalog::instance().getRegisteredQueries();
}

const map<string, QueryCatalogEntryPtr> CoordinatorService::getRunningQueries() {
    return QueryCatalog::instance().getQueries(QueryStatus::Running);
}
