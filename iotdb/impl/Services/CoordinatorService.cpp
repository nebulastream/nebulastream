#include <Services/CoordinatorService.hpp>
#include <Actors/ExecutableTransferObject.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace iotdb;
using namespace std;

FogTopologyEntryPtr CoordinatorService::register_sensor(const string &ip, uint16_t publish_port,
                                                        uint16_t receive_port, int cpu, const string &sensor_type) {
  FogTopologyManager &topologyManager = this->_topologyManagerPtr->getInstance();
  FogTopologySensorNodePtr sensorNode = topologyManager.createFogSensorNode("ip", CPUCapacity::Value(cpu));
  sensorNode->setSensorType(sensor_type);
  sensorNode->setIp(ip);
  sensorNode->setPublishPort(publish_port);
  sensorNode->setReceivePort(receive_port);
  const FogTopologyEntryPtr &kRootNode = FogTopologyManager::getInstance().getRootNode();
  topologyManager.createFogTopologyLink(sensorNode, kRootNode);
  return sensorNode;
}

string CoordinatorService::register_query(const string &description, const string &strategy) {
  FogExecutionPlan fogExecutionPlan;
  if (this->_registeredQueries.find(description) == this->_registeredQueries.end() &&
      this->_runningQueries.find(description) == this->_runningQueries.end()) {
    // if query is currently not registered or running
    IOTDB_INFO("CoordinatorService: Registering query " << description << " with strategy " << strategy);
    FogOptimizer queryOptimizer;
    FogTopologyPlanPtr topologyPlan = this->_topologyManagerPtr->getInstance().getTopologyPlan();

    // currently only one hard coded example query is supported
    try {
      if (description == "example") {
        Schema schema = Schema::create()
            .addField("id", BasicType::UINT32)
            .addField("value", BasicType::UINT64);
        Stream stream = Stream("cars", schema);

        InputQuery inputQuery = InputQuery::from(stream)
            .filter(stream["value"] > 42)
            .print(std::cout);

        fogExecutionPlan = queryOptimizer.prepareExecutionGraph(strategy, inputQuery, topologyPlan);
        std::string queryId = boost::uuids::to_string(boost::uuids::random_generator()());

        tuple<Schema, FogExecutionPlan> t = std::make_tuple(schema, fogExecutionPlan);
        this->_registeredQueries.insert({queryId, t});
        IOTDB_INFO("FogExecutionPlan: " << fogExecutionPlan.getTopologyPlanString());
        return queryId;
      } else {
        // TODO: implement
        IOTDB_INFO("CoordinatorService: Registration failed! Only query example is supported!");
      }
    }
    catch (const std::exception &ex) {
      IOTDB_ERROR("CoordinatorService(" << description << "):" << ex.what())
    } catch (const std::string &ex) {
      IOTDB_ERROR("CoordinatorService(" << description << "):" << ex)
    } catch (...) {
      IOTDB_ERROR("CoordinatorService(" << description << "): Unknown exception during registration!")
    }
  } else if (this->_registeredQueries.find(description) != this->_registeredQueries.end()) {
    IOTDB_INFO("CoordinatorService: Query is already registered -> " << description);
  } else {
    IOTDB_INFO("CoordinatorService: Query is already running -> " << description);
  }
  return nullptr;
}

bool CoordinatorService::deregister_query(const string &queryId) {
  bool out = false;
  if (this->_registeredQueries.find(queryId) == this->_registeredQueries.end() &&
      this->_runningQueries.find(queryId) == this->_runningQueries.end()) {
    IOTDB_INFO("CoordinatorService: No deletion required! Query has neither been registered or deployed->" << queryId);
  } else if (this->_registeredQueries.find(queryId) != this->_registeredQueries.end()) {
    // Query is registered, but not running -> just remove from registered queries
    get<1>(this->_registeredQueries.at(queryId)).freeResources(1);
    this->_registeredQueries.erase(queryId);
    IOTDB_INFO("CoordinatorService: Query was registered and has been succesfully removed -> " << queryId);
  } else {
    IOTDB_INFO("CoordinatorService: Deregistering running query..");
    //Query is running -> stop query locally if it is running and free resources
    get<1>(this->_runningQueries.at(queryId)).freeResources(1);
    this->_runningQueries.erase(queryId);
    IOTDB_INFO("CoordinatorService:  successfully removed query " << queryId);
    out = true;
  }
  return out;
}

unordered_map<FogTopologyEntryPtr,
              ExecutableTransferObject> CoordinatorService::make_deployment(const string &queryId) {
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> output;
  if (this->_registeredQueries.find(queryId) != this->_registeredQueries.end() &&
      this->_runningQueries.find(queryId) == this->_runningQueries.end()) {
    IOTDB_INFO("CoordinatorService: Deploying query " << queryId);
    // get the schema and FogExecutionPlan stored during query registration
    Schema schema = get<0>(this->_registeredQueries.at(queryId));
    FogExecutionPlan execPlan = get<1>(this->_registeredQueries.at(queryId));

    //iterate through all vertices in the topology
    for (const ExecutionVertex &v : execPlan.getExecutionGraph()->getAllVertex()) {
      OperatorPtr operators = v.ptr->getRootOperator();
      if (operators) {
        // if node contains operators to be deployed -> serialize and send them to the according node
        vector<DataSourcePtr> sources = getSources(queryId, v);
        vector<DataSinkPtr> destinations = getSinks(queryId, v);
        FogTopologyEntryPtr fogNode = v.ptr->getFogNode();
        ExecutableTransferObject eto = ExecutableTransferObject(queryId, schema, sources, destinations, operators);
        output.insert({fogNode, eto});
      }
    }
    // move registered query to running query
    tuple<Schema, FogExecutionPlan> t = std::make_tuple(schema, execPlan);
    this->_runningQueries.insert({queryId, t});
    this->_registeredQueries.erase(queryId);
  } else if (this->_runningQueries.find(queryId) != this->_runningQueries.end()) {
    IOTDB_WARNING("CoordinatorService: Query is already running -> " << queryId);
  } else {
    IOTDB_WARNING("CoordinatorService: Query is not registered -> " << queryId);
  }
  return output;
}

vector<DataSourcePtr> CoordinatorService::getSources(const string &description, const ExecutionVertex &v) {
  vector<DataSourcePtr> out = vector<DataSourcePtr>();
  Schema schema = get<0>(this->_registeredQueries.at(description));
  FogExecutionPlan execPlan = get<1>(this->_registeredQueries.at(description));

  DataSourcePtr source;
  if (execPlan.getExecutionGraph()->getAllEdgesToNode(v.ptr).empty()) {
    // TODO: check if has source operator
    // if (v.ptr->getRootOperator()->getOperatorType() == SOURCE_OP){
    source = createTestDataSourceWithSchema(schema);
  } else {
    // create local zmq source
    const FogTopologyEntryPtr &kRootNode = FogTopologyManager::getInstance().getRootNode();
    source = createZmqSource(schema, kRootNode->getIp(), assign_port(description));
  }
  out.emplace_back(source);
  return out;
}

vector<DataSinkPtr> CoordinatorService::getSinks(const string &description, const ExecutionVertex &v) {
  vector<DataSinkPtr> out = vector<DataSinkPtr>();
  Schema schema = get<0>(this->_registeredQueries.at(description));
  FogExecutionPlan execPlan = get<1>(this->_registeredQueries.at(description));

  DataSinkPtr sink;
  if (execPlan.getExecutionGraph()->getAllEdgesFromNode(v.ptr).empty()) {
    // TODO: check if has sink operator
    // if (v.ptr->getRootOperator()->getOperatorType() == SOURCE_OP){
    sink = createPrintSinkWithSink(schema, std::cout);
  } else {
    // create local zmq source
    const FogTopologyEntryPtr &kRootNode = FogTopologyManager::getInstance().getRootNode();
    sink = createZmqSink(schema, kRootNode->getIp(), assign_port(description));
  }
  out.emplace_back(sink);
  return out;
}

int CoordinatorService::assign_port(const string &description) {
  if (this->_queryToPort.find(description) != this->_queryToPort.end()) {
    return this->_queryToPort.at(description);
  } else {
    // increase max port in map by 1
    const FogTopologyEntryPtr &kRootNode = FogTopologyManager::getInstance().getRootNode();
    uint16_t kFreeZmqPort = kRootNode->getNextFreeReceivePort();
    this->_queryToPort.insert({description, kFreeZmqPort});
    return kFreeZmqPort;
  }
}

string CoordinatorService::executeQuery(const string userQuery, const string &optimizationStrategyName) {

  std::string queryId = register_query(userQuery, optimizationStrategyName);
  make_deployment(queryId);
  return queryId;
}

bool CoordinatorService::deregister_sensor(const FogTopologyEntryPtr &entry) {
  return this->_topologyManagerPtr->getInstance().removeFogNode(entry);
}
string CoordinatorService::getTopologyPlanString() {
  return this->_topologyManagerPtr->getInstance().getTopologyPlanString();
}

FogExecutionPlan CoordinatorService::getRegisteredQuery(string queryId) {
  if (this->_registeredQueries.find(queryId) != this->_registeredQueries.end()) {
    return get<1>(this->_registeredQueries.at(queryId));
  }
}

bool CoordinatorService::clearQueryCatalogs() {
  try {
    _registeredQueries.clear();
    _runningQueries.clear();
  } catch (...) {
    return false;
  }
  return true;
}

const unordered_map<string, tuple<Schema, FogExecutionPlan>> &CoordinatorService::getRegisteredQueries() const {
  return _registeredQueries;
}

const unordered_map<string, tuple<Schema, FogExecutionPlan>> &CoordinatorService::getRunningQueries() const {
  return _runningQueries;
}
