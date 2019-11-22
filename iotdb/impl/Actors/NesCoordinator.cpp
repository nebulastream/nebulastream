#include <Actors/NesCoordinator.hpp>
#include <Actors/ExecutableTransferObject.hpp>

namespace iotdb {
NesCoordinator::NesCoordinator(const std::string &ip, uint16_t publish_port, uint16_t receive_port) {
  this->_ip = ip;
  this->_publish_port = publish_port;
  this->_receive_port = receive_port;

  this->_topologyManagerPtr->getInstance().resetFogTopologyPlan();
  auto coordinatorNode = this->_topologyManagerPtr->getInstance().createFogCoordinatorNode(ip, CPUCapacity::HIGH);
  coordinatorNode->setPublishPort(publish_port);
  coordinatorNode->setReceivePort(receive_port);
  this->_thisEntry = coordinatorNode;
}

FogTopologyEntryPtr NesCoordinator::register_sensor(const string &ip, uint16_t publish_port,
                                                    uint16_t receive_port, int cpu, const string &sensor_type) {
  FogTopologyManager &topologyManager = this->_topologyManagerPtr->getInstance();
  FogTopologySensorNodePtr sensorNode = topologyManager.createFogSensorNode("ip", CPUCapacity::Value(cpu));
  sensorNode->setSensorType(sensor_type);
  sensorNode->setIp(ip);
  sensorNode->setPublishPort(publish_port);
  sensorNode->setReceivePort(receive_port);
  topologyManager.createFogTopologyLink(sensorNode, this->_thisEntry);
  return sensorNode;
}

vector<string> NesCoordinator::getOperators() {
  vector<string> result;
  for (auto const &x : this->_runningQueries) {
    // iterate through all running queries and get operators
    string str_opts;
    tuple<Schema, FogExecutionPlan> elements = x.second;

    OperatorPtr operators;
    // find operators of _this in execution graph
    for (const ExecutionVertex &v: get<1>(elements).getExecutionGraph()->getAllVertex()) {
      if (v.ptr->getFogNode() == this->_thisEntry) {
        operators = v.ptr->getRootOperator();
        break;
      }
    }

    if (operators) {
      // if has operators convert them to a flattened string list and return
      set<OperatorType> flattened = operators->flattenedTypes();
      for (const OperatorType &_o: flattened) {
        if (!str_opts.empty())
          str_opts.append(", ");
        str_opts.append(operatorTypeToString.at(_o));
      }
      result.emplace_back(x.first + "->" + str_opts);
    }
  }
  return result;
}

FogExecutionPlan NesCoordinator::register_query(const string &description,
                                                const string &sensor_type,
                                                const string &strategy) {
  FogExecutionPlan fogExecutionPlan;
  if (this->_registeredQueries.find(description) == this->_registeredQueries.end() &&
      this->_runningQueries.find(description) == this->_runningQueries.end()) {
    // if query is currently not registered or running
    IOTDB_INFO("Registering query " << description << " with strategy " << strategy);
    FogOptimizer queryOptimizer;
    FogTopologyPlanPtr topologyPlan = this->_topologyManagerPtr->getInstance().getTopologyPlan();

    // currently only one hard coded example query is supported
    try {
      if (description == "example") {
        Schema schema = Schema::create()
            .addField("id", BasicType::UINT32)
            .addField("value", BasicType::UINT64);
        Stream stream = Stream(sensor_type, schema);

        InputQuery &inputQuery = InputQuery::from(stream)
            .filter(stream["value"] > 42)
            .print(std::cout);

        fogExecutionPlan = queryOptimizer.prepareExecutionGraph(strategy,
                                                                inputQuery,
                                                                topologyPlan);

        tuple<Schema, FogExecutionPlan> t = std::make_tuple(schema, fogExecutionPlan);
        this->_registeredQueries.insert({description, t});
        IOTDB_INFO("FogExecutionPlan: " << fogExecutionPlan.getTopologyPlanString());
      } else {
        // TODO: implement
        IOTDB_INFO("Registration failed! Only query example is supported!");
      }
    }
    catch (...) {
      //aout(this) << ": " << ex.what() << endl;
    }
  } else if (this->_registeredQueries.find(description) != this->_registeredQueries.end()) {
    IOTDB_INFO("Query is already registered -> " << description);
  } else {
    IOTDB_INFO("Query is already running -> " << description);
  }
  return fogExecutionPlan;
}

bool NesCoordinator::deregister_query(const string &description) {
  bool out = false;
  if (this->_registeredQueries.find(description) == this->_registeredQueries.end() &&
      this->_runningQueries.find(description) == this->_runningQueries.end()) {
    IOTDB_INFO("*** No deletion required! Query has neither been registered or deployed->" << description);
  } else if (this->_registeredQueries.find(description) != this->_registeredQueries.end()) {
    // Query is registered, but not running -> just remove from registered queries
    get<1>(this->_registeredQueries.at(description)).freeResources(1);
    this->_registeredQueries.erase(description);
    IOTDB_INFO("Query was registered and has been succesfully removed -> " << description);
  } else {
    IOTDB_INFO("Deregistering running query...");
    //Query is running -> stop query locally if it is running and free resources
    get<1>(this->_runningQueries.at(description)).freeResources(1);
    this->_runningQueries.erase(description);
    IOTDB_INFO("*** successfully removed query " << description);
    out = true;
  }
  return out;
}

unordered_map<FogTopologyEntryPtr,
              ExecutableTransferObject> NesCoordinator::make_deployment(const string &description) {
  unordered_map<FogTopologyEntryPtr, ExecutableTransferObject> output;
  if (this->_registeredQueries.find(description) != this->_registeredQueries.end() &&
      this->_runningQueries.find(description) == this->_runningQueries.end()) {
    IOTDB_INFO("Deploying query " << description);
    // get the schema and FogExecutionPlan stored during query registration
    Schema schema = get<0>(this->_registeredQueries.at(description));
    FogExecutionPlan execPlan = get<1>(this->_registeredQueries.at(description));

    //iterate through all vertices in the topology
    for (const ExecutionVertex &v : execPlan.getExecutionGraph()->getAllVertex()) {
      OperatorPtr operators = v.ptr->getRootOperator();
      if (operators) {
        // if node contains operators to be deployed -> serialize and send them to the according node
        vector<DataSourcePtr> sources = getSources(description, v);
        vector<DataSinkPtr> destinations = getSinks(description, v);
        FogTopologyEntryPtr fogNode = v.ptr->getFogNode();
        ExecutableTransferObject eto = ExecutableTransferObject(description, schema, sources, destinations, operators);
        output.insert({fogNode, eto});
      }
    }
    // move registered query to running query
    tuple<Schema, FogExecutionPlan> t = std::make_tuple(schema, execPlan);
    this->_runningQueries.insert({description, t});
    this->_registeredQueries.erase(description);
  } else if (this->_runningQueries.find(description) != this->_runningQueries.end()) {
    IOTDB_WARNING("Query is already running -> " << description);
  } else {
    IOTDB_WARNING("Query is not registered -> " << description);
  }
  return output;
}

vector<DataSourcePtr> NesCoordinator::getSources(const string &description, const ExecutionVertex &v) {
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
    source = createZmqSource(schema, this->_ip, assign_port(description));
  }
  out.emplace_back(source);
  return out;
}

vector<DataSinkPtr> NesCoordinator::getSinks(const string &description, const ExecutionVertex &v) {
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
    sink = createZmqSink(schema, this->_ip, assign_port(description));
  }
  out.emplace_back(sink);
  return out;
}

int NesCoordinator::assign_port(const string &description) {
  if (this->_queryToPort.find(description) != this->_queryToPort.end()) {
    return this->_queryToPort.at(description);
  } else {
    // increase max port in map by 1
    this->_receive_port += 1;
    this->_queryToPort.insert({description, this->_receive_port});
    return this->_receive_port;
  }
}

const FogTopologyEntryPtr &NesCoordinator::getThisEntry() const {
  return _thisEntry;
}

bool NesCoordinator::deregister_sensor(const FogTopologyEntryPtr &entry) {
  return this->_topologyManagerPtr->getInstance().removeFogNode(entry);
}
string NesCoordinator::getTopologyPlanString() {
  return this->_topologyManagerPtr->getInstance().getTopologyPlanString();
}

const unordered_map<string, tuple<Schema, FogExecutionPlan>> &NesCoordinator::getRegisteredQueries() const {
  return _registeredQueries;
}

const unordered_map<string, tuple<Schema, FogExecutionPlan>> &NesCoordinator::getRunningQueries() const {
  return _runningQueries;
}

}

