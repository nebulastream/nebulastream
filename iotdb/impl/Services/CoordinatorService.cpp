#include <Services/CoordinatorService.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/Logger.hpp>

using namespace NES;
using namespace std;

string CoordinatorService::getNodePropertiesAsString(
    const NESTopologyEntryPtr &entry) {
  return entry->getNodeProperty();
}

NESTopologyEntryPtr CoordinatorService::register_sensor(
    const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
    const string &nodeProperties, PhysicalStreamConfig streamConf) {
  NESTopologyManager &topologyManager = this->topologyManagerPtr->getInstance();
  NESTopologySensorNodePtr sensorNode = topologyManager.createNESSensorNode(
      ip, CPUCapacity::Value(cpu));

  sensorNode->setPhysicalStreamName(streamConf.physicalStreamName);
  sensorNode->setPublishPort(publish_port);
  sensorNode->setReceivePort(receive_port);
  if (nodeProperties != "defaultProperties")
    sensorNode->setNodeProperty(nodeProperties);

  NES_DEBUG(
      "try to register sensor phyName=" << streamConf.physicalStreamName << " logName=" << streamConf.logicalStreamName << " nodeID=" << sensorNode->getId())
  //check if logical stream exists
  if (!StreamCatalog::instance().testIfLogicalStreamExistsInSchemaMapping(
      streamConf.logicalStreamName)) {
    NES_ERROR(
        "Coordinator: error logical stream" << streamConf.logicalStreamName << " does not exist when adding physical stream " << streamConf.physicalStreamName)
    throw Exception(
        "logical stream does not exist " + streamConf.logicalStreamName);
  }
  SchemaPtr schem = StreamCatalog::instance().getSchemaForLogicalStream(
      streamConf.logicalStreamName);

  DataSourcePtr source;
  if (streamConf.sourceType != "CSVSource"
      && streamConf.sourceType != "OneGeneratorSource") {
    NES_ERROR(
        "Coordinator: error source type " << streamConf.sourceType << " is not supported")
    throw Exception(
        "Coordinator: error source type " + streamConf.sourceType
            + " is not supported");
  }
  StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(
      streamConf.sourceType, streamConf.sourceConfig, sensorNode,
      streamConf.physicalStreamName);

  bool success = StreamCatalog::instance().addPhysicalStream(
      streamConf.logicalStreamName, sce);
  if (!success) {
    NES_ERROR(
        "Coordinator: physical stream " << streamConf.physicalStreamName << " could not be added to catalog")
    throw Exception(
        "Coordinator: physical stream " + streamConf.physicalStreamName
            + " could not be added to catalog");

  }

  const NESTopologyEntryPtr &kRootNode = NESTopologyManager::getInstance()
      .getRootNode();
  topologyManager.createNESTopologyLink(sensorNode, kRootNode);
  return sensorNode;
}

string CoordinatorService::register_query(const string &queryString,
                                          const string &strategy) {

  NES_INFO(
      "CoordinatorService: Registering query " << queryString << " with strategy " << strategy);

  if (queryString.find("Stream(") != std::string::npos) {
    NES_ERROR(
        "CoordinatorService: queries are not allowed to specify streams anymore.")
    throw Exception("Queries are not allowed to define streams anymore");
  }
  if (queryString.find("Schema::create()") != std::string::npos) {
    NES_ERROR(
        "CoordinatorService: queries are not allowed to specify schemas anymore.")
    throw Exception("Queries are not allowed to define schemas anymore");
  }

  try {
    InputQueryPtr inputQueryPtr = queryService.getInputQueryFromQueryString(
        queryString);
    Schema schema = inputQueryPtr->source_stream->getSchema();

    const NESExecutionPlan &kExecutionPlan = optimizerService.getExecutionPlan(
        inputQueryPtr, strategy);
    NES_DEBUG(
        "OptimizerService: Final Execution Plan =" << kExecutionPlan.getTopologyPlanString())

    std::string queryId = boost::uuids::to_string(
        boost::uuids::random_generator()());
    tuple<Schema, NESExecutionPlan> t = std::make_tuple(schema, kExecutionPlan);
    this->registeredQueries.insert( { queryId, t });
    return queryId;
  } catch (...) {
    NES_ERROR(
        "Unable to process input request with: queryString: " << queryString << "\n strategy: " << strategy);
    return nullptr;
  }
}

bool CoordinatorService::deregister_query(const string &queryId) {
  bool out = false;
  if (this->registeredQueries.find(queryId) == this->registeredQueries.end()
      && this->runningQueries.find(queryId) == this->runningQueries.end()) {
    NES_INFO(
        "CoordinatorService: No deletion required! Query has neither been registered or deployed->" << queryId);
  } else if (this->registeredQueries.find(queryId)
      != this->registeredQueries.end()) {
    // Query is registered, but not running -> just remove from registered queries
    get<1>(this->registeredQueries.at(queryId)).freeResources();
    this->registeredQueries.erase(queryId);
    NES_INFO(
        "CoordinatorService: Query was registered and has been successfully removed -> " << queryId);
  } else {
    NES_INFO("CoordinatorService: De-registering running query..");
    //Query is running -> stop query locally if it is running and free resources
    get<1>(this->runningQueries.at(queryId)).freeResources();
    this->runningQueries.erase(queryId);
    NES_INFO("CoordinatorService:  successfully removed query " << queryId);
    out = true;
  }
  return out;
}

unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> CoordinatorService::make_deployment(
    const string &queryId) {
  unordered_map<NESTopologyEntryPtr, ExecutableTransferObject> output;
  if (this->registeredQueries.find(queryId) != this->registeredQueries.end()
      && this->runningQueries.find(queryId) == this->runningQueries.end()) {
    NES_INFO("CoordinatorService: Deploying query " << queryId);
    // get the schema and NESExecutionPlan stored during query registration
    Schema schema = get<0>(this->registeredQueries.at(queryId));
    NESExecutionPlan execPlan = get<1>(this->registeredQueries.at(queryId));

    //iterate through all vertices in the topology
    for (const ExecutionVertex &v : execPlan.getExecutionGraph()->getAllVertex()) {
      OperatorPtr operators = v.ptr->getRootOperator();
      if (operators) {
        // if node contains operators to be deployed -> serialize and send them to the according node
        vector<DataSourcePtr> sources = getSources(queryId, v);
        vector<DataSinkPtr> destinations = getSinks(queryId, v);
        NESTopologyEntryPtr nesNode = v.ptr->getNESNode();
        ExecutableTransferObject eto = ExecutableTransferObject(queryId, schema,
                                                                sources,
                                                                destinations,
                                                                operators);
        output.insert( { nesNode, eto });
      }
    }
    // move registered query to running query
    tuple<Schema, NESExecutionPlan> t = std::make_tuple(schema, execPlan);
    this->runningQueries.insert( { queryId, t });
    this->registeredQueries.erase(queryId);
  } else if (this->runningQueries.find(queryId) != this->runningQueries.end()) {
    NES_WARNING("CoordinatorService: Query is already running -> " << queryId);
  } else {
    NES_WARNING("CoordinatorService: Query is not registered -> " << queryId);
  }
  return output;
}

vector<DataSourcePtr> CoordinatorService::getSources(const string &queryId,
                                                     const ExecutionVertex &v) {
  vector<DataSourcePtr> out = vector<DataSourcePtr>();
  Schema schema = get<0>(this->registeredQueries.at(queryId));
  NESExecutionPlan execPlan = get<1>(this->registeredQueries.at(queryId));

  DataSourcePtr source = findDataSourcePointer(v.ptr->getRootOperator());

  //FIXME: what about user defined a ZMQ sink?
  if (source->getType() == ZMQ_SOURCE) {
    //FIXME: Maybe a better way to do it? perhaps type cast to ZMQSource type and just update the port number
    // create local zmq source
    const NESTopologyEntryPtr &kRootNode = NESTopologyManager::getInstance()
        .getRootNode();
    source = createZmqSource(schema, kRootNode->getIp(), assign_port(queryId));
  }
  out.emplace_back(source);
  return out;
}

vector<DataSinkPtr> CoordinatorService::getSinks(const string &queryId,
                                                 const ExecutionVertex &v) {

  vector<DataSinkPtr> out = vector<DataSinkPtr>();
  Schema schema = get<0>(this->registeredQueries.at(queryId));
  NESExecutionPlan execPlan = get<1>(this->registeredQueries.at(queryId));
  DataSinkPtr sink = findDataSinkPointer(v.ptr->getRootOperator());

  //FIXME: what about user defined a ZMQ sink?
  if (sink->getType() == ZMQ_SINK) {
    //FIXME: Maybe a better way to do it? perhaps type cast to ZMQSink type and just update the port number
    //create local zmq sink
    const NESTopologyEntryPtr &kRootNode = NESTopologyManager::getInstance()
        .getRootNode();
    sink = createZmqSink(schema, kRootNode->getIp(), assign_port(queryId));
  }
  out.emplace_back(sink);
  return out;
}

DataSinkPtr CoordinatorService::findDataSinkPointer(OperatorPtr operatorPtr) {

  if (operatorPtr->parent == nullptr
      && operatorPtr->getOperatorType() == SINK_OP) {
    SinkOperator *sinkOperator = dynamic_cast<SinkOperator*>(operatorPtr.get());
    return sinkOperator->getDataSinkPtr();
  } else if (operatorPtr->parent != nullptr) {
    return findDataSinkPointer(operatorPtr->parent);
  } else {
    NES_WARNING("Found query graph without a SINK.");
    return nullptr;
  }
}

DataSourcePtr CoordinatorService::findDataSourcePointer(
    OperatorPtr operatorPtr) {
  vector<OperatorPtr> &children = operatorPtr->childs;
  if (children.empty() && operatorPtr->getOperatorType() == SOURCE_OP) {
    SourceOperator *sourceOperator = dynamic_cast<SourceOperator*>(operatorPtr
        .get());
    return sourceOperator->getDataSourcePtr();
  } else if (!children.empty()) {
    //FIXME: What if there are more than one source?

    for (OperatorPtr child : children) {
      return findDataSourcePointer(operatorPtr->parent);
    }
  }
  NES_WARNING("Found query graph without a SOURCE.");
  return nullptr;
}

int CoordinatorService::assign_port(const string &queryId) {
  if (this->queryToPort.find(queryId) != this->queryToPort.end()) {
    return this->queryToPort.at(queryId);
  } else {
    // increase max port in map by 1
    const NESTopologyEntryPtr &kRootNode = NESTopologyManager::getInstance()
        .getRootNode();
    uint16_t kFreeZmqPort = kRootNode->getNextFreeReceivePort();
    this->queryToPort.insert( { queryId, kFreeZmqPort });
    return kFreeZmqPort;
  }
}

bool CoordinatorService::deregister_sensor(const NESTopologyEntryPtr &entry) {

  return this->topologyManagerPtr->getInstance().removeNESNode(entry);

}
string CoordinatorService::getTopologyPlanString() {
  return this->topologyManagerPtr->getInstance().getNESTopologyPlanString();
}

NESExecutionPlan* CoordinatorService::getRegisteredQuery(string queryId) {
  if (this->registeredQueries.find(queryId) != this->registeredQueries.end()) {
    return &(get<1>(this->registeredQueries.at(queryId)));
  }
  return nullptr;
}

bool CoordinatorService::clearQueryCatalogs() {
  try {
    registeredQueries.clear();
    runningQueries.clear();
  } catch (...) {
    return false;
  }
  return true;
}

const unordered_map<string, tuple<Schema, NESExecutionPlan>>& CoordinatorService::getRegisteredQueries() const {
  return registeredQueries;
}

const unordered_map<string, tuple<Schema, NESExecutionPlan>>& CoordinatorService::getRunningQueries() const {
  return runningQueries;
}
