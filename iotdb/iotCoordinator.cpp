#include <iostream>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>
#include <Network/atom_utils.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>

#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <Util/ErrorHandling.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/SourceOperator.hpp>
#include <Util/UtilityFunctions.hpp>

#include <string>
#include <Topology/FogTopologyManager.hpp>
#include <Optimizer/FogOptimizer.hpp>
#include <utility>
#include <stdint.h>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::unordered_map;

using namespace caf;
using namespace iotdb;

namespace iotdb {
constexpr auto task_timeout = std::chrono::seconds(10);

// class-based, statically typed, event-based API
// the client queues pending tasks
struct coordinator_state {
  string ip;
  uint16_t publish_port;
  uint16_t receive_port;
  FogTopologyEntryPtr _thisEntry;

  NodeEngine *enginePtr;

  unordered_map<strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
  unordered_map<FogTopologyEntryPtr, strong_actor_ptr> topologyActorMap;
  unordered_map<string, int> queryToPort;

  shared_ptr<FogTopologyManager> topologyManagerPtr;
  unordered_map<string, tuple<Schema, FogExecutionPlan>> registeredQueries;
  unordered_map<string, tuple<Schema, FogExecutionPlan, QueryExecutionPlanPtr>> runningQueries;
};

class coordinator : public stateful_actor<coordinator_state> {
 public:
  explicit coordinator(actor_config &cfg, string ip, uint16_t publish_port, uint16_t receive_port)
      : stateful_actor(cfg) {
    this->state.ip = ip;
    this->state.publish_port = publish_port;
    this->state.receive_port = receive_port;

    this->state.topologyManagerPtr->getInstance().resetFogTopologyPlan();
    FogTopologyCoordinatorNodePtr coordinatorNode =
        this->state.topologyManagerPtr->getInstance().createFogCoordinatorNode(ip, CPUCapacity::HIGH);
    coordinatorNode->setPublishPort(publish_port);
    coordinatorNode->setReceivePort(receive_port);
    this->state._thisEntry = coordinatorNode;

    this->state.actorTopologyMap.insert({this->address().get(), coordinatorNode});
    this->state.topologyActorMap.insert({coordinatorNode, this->address().get()});

    this->state.enginePtr = new NodeEngine();
    this->state.enginePtr->start();
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  // function-based, statically typed, event-based API
  behavior init() {
    // transition to `unconnected` on server failure
    this->set_down_handler([=](const down_msg &dm) {
      strong_actor_ptr key = dm.source.get();
      auto hdl = actor_cast<actor>(key);
      if (this->state.actorTopologyMap.find(key) != this->state.actorTopologyMap.end()) {
        // remove disconnected worker from topology
        this->state.topologyManagerPtr->getInstance().removeFogNode(this->state.actorTopologyMap.at(key));
        this->state.topologyActorMap.erase(this->state.actorTopologyMap.at(key));
        this->state.actorTopologyMap.erase(key);
        aout(this) << "*** lost connection to worker " << key << endl;
        key->get()->unregister_from_system();
      }
    });
    return running();
  }

  behavior running() {
    return {
        // framework internal rpcs
        [=](register_worker_atom, string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
            const strong_actor_ptr &sap) {
          this->register_worker(ip, publish_port, receive_port, cpu, sap);
        },
        [=](register_sensor_atom, string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
            const string &sensor_type, const strong_actor_ptr &sap) {
          this->register_sensor(ip, publish_port, receive_port, cpu, sensor_type, sap);
        },
        [=](register_query_atom, const string &query_string, const string &sensor_type,
            const string &optimizationStrategyName) {
          this->register_query(query_string, sensor_type, optimizationStrategyName);
        },
        [=](deploy_query_atom, const string &query_desc, const string &sensor_type) {
          this->deploy_query(query_desc, sensor_type);
        },
        [=](delete_query_atom, const string &query_desc, const string &sensor_type) {
          this->delete_query(query_desc, sensor_type);
        },
        [=](execute_query_atom, const string &query, string &str_schema, vector<string> &str_sources,
            vector<string> &str_destinations, string &str_ops) {
          this->execute_query(query, str_schema, str_sources, str_destinations, str_ops);
        },
        [=](get_operators_atom) {
          return this->getOperators();
        },
        // external methods for users
        [=](topology_json_atom) {
          string topo = this->state.topologyManagerPtr->getInstance().getTopologyPlanString();
          aout(this) << "Printing Topology" << endl;
          aout(this) << topo << endl;
        },
        [=](show_registered_atom) {
          aout(this) << "Printing Registered Queries" << endl;
          for (const auto &p : this->state.registeredQueries) {
            aout(this) << p.first << endl;
          }
        },
        [=](show_running_atom) {
          aout(this) << "Printing Running Queries" << endl;
          for (const auto &p : this->state.runningQueries) {
            aout(this) << p.first << endl;
          }
        },
        [=](show_running_operators_atom) {
          aout(this) << "Requesting deployed operators from connected devices.." << endl;
          this->show_operators();
        }
    };
  }

  void register_worker(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                       const strong_actor_ptr &sap) {
    auto hdl = actor_cast<actor>(sap);
    FogTopologyManager &topologyManager = this->state.topologyManagerPtr->getInstance();
    FogTopologyWorkerNodePtr workerNode = topologyManager.createFogWorkerNode(ip, CPUCapacity::Value(cpu));
    workerNode->setPublishPort(publish_port);
    workerNode->setReceivePort(receive_port);

    topologyManager.createFogTopologyLink(workerNode, this->state.actorTopologyMap.at(this->address().get()));
    this->state.actorTopologyMap.insert({sap, workerNode});
    this->state.topologyActorMap.insert({workerNode, sap});
    this->monitor(hdl);
    aout(this) << "*** successfully registered worker (CPU=" << cpu << ") " << to_string(hdl) << endl;
  }

  void register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                       const string &sensor_type, const strong_actor_ptr &sap) {
    auto hdl = actor_cast<actor>(sap);
    FogTopologyManager &topologyManager = this->state.topologyManagerPtr->getInstance();
    FogTopologySensorNodePtr sensorNode = topologyManager.createFogSensorNode("ip", CPUCapacity::Value(cpu));
    sensorNode->setSensorType(sensor_type);
    sensorNode->setIp(ip);
    sensorNode->setPublishPort(publish_port);
    sensorNode->setReceivePort(receive_port);

    topologyManager.createFogTopologyLink(sensorNode, this->state.actorTopologyMap.at(this->address().get()));
    this->state.actorTopologyMap.insert({sap, sensorNode});
    this->state.topologyActorMap.insert({sensorNode, sap});
    this->monitor(hdl);
    aout(this) << "*** successfully registered sensor (CPU=" << cpu << ", Type: "
               << sensorNode->getSensorType() << ") "
               << to_string(hdl) << endl;
  }

  vector<string> getOperators() {
    vector<string> result;
    for (auto const &x : this->state.runningQueries) {
      string str_opts;
      tuple<Schema, FogExecutionPlan, QueryExecutionPlanPtr> elements = x.second;

      OperatorPtr operators;
      for (const ExecutionVertex &v: get<1>(elements).getExecutionGraph()->getAllVertex()) {
        if (v.ptr->getFogNode() == this->state._thisEntry) {
          operators = v.ptr->getRootOperator();
          break;
        }
      }

      if (operators) {
        vector<OperatorType> flattened = operators->flattenedTypes();
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

  void show_operators() {
    //send messages to all connected devices and get their operators
    for (auto const &x : this->state.actorTopologyMap) {
      strong_actor_ptr sap = x.first;
      auto hdl = actor_cast<actor>(sap);

      this->request(hdl, task_timeout, get_operators_atom::value).then(
          [=](const vector<string> &vec) {
            std::ostringstream ss;
            ss << x.second->getEntryTypeString() << "::" << to_string(hdl) << ":" << endl;

            aout(this) << ss.str() << vec << endl;
          },
          [=](const error &) {
            aout(this) << "Error for " << to_string(hdl) << endl;
          }
      );
    }
  }

  void register_query(const string &query, const string &sensor_type, const string &optimizationStrategyName) {
    if (this->state.registeredQueries.find(query + sensor_type) == this->state.registeredQueries.end() &&
        this->state.runningQueries.find(query + sensor_type) == this->state.runningQueries.end()) {
      aout(this) << "Registering query " << query << "-" << sensor_type << " with strategy "
                 << optimizationStrategyName << endl;
      FogExecutionPlan fogExecutionPlan;
      FogOptimizer queryOptimizer;
      FogTopologyPlanPtr topologyPlan = this->state.topologyManagerPtr->getInstance().getTopologyPlan();

      try {
        if (query == "example") {
          Schema schema = Schema::create()
              .addField("id", BasicType::UINT32)
              .addField("value", BasicType::UINT64);
          Stream stream = Stream(sensor_type, schema);

          InputQuery &inputQuery = InputQuery::from(stream)
              .filter(stream["value"] > 42)
              .print(std::cout);

          fogExecutionPlan = queryOptimizer.prepareExecutionGraph(optimizationStrategyName,
                                                                  inputQuery,
                                                                  topologyPlan);

          tuple<Schema, FogExecutionPlan> t = std::make_tuple(schema, fogExecutionPlan);
          this->state.registeredQueries.insert({query + sensor_type, t});
          aout(this) << "FogExecutionPlan: " << fogExecutionPlan.getTopologyPlanString() << endl;
        } else {
          // TODO: implement
          aout(this) << "Registration failed! Only query example is supported!" << endl;
        }
      }
      catch (const std::exception &ex) {
        aout(this) << ": " << ex.what() << endl;
      }
    } else if (this->state.registeredQueries.find(query + sensor_type) != this->state.registeredQueries.end()) {
      aout(this) << "Query is already registered -> " << query << "-" << sensor_type << endl;
    } else {
      aout(this) << "Query is already running -> " << query << "-" << sensor_type << endl;
    }
  }

  void deploy_query(const string &query, const string &sensor_type) {
    if (this->state.registeredQueries.find(query + sensor_type) != this->state.registeredQueries.end() &&
        this->state.runningQueries.find(query + sensor_type) == this->state.runningQueries.end()) {
      aout(this) << "Deploying query " << query << "-" << sensor_type << endl;
      Schema schema = get<0>(this->state.registeredQueries.at(query + sensor_type));
      FogExecutionPlan execPlan = get<1>(this->state.registeredQueries.at(query + sensor_type));

      for (const ExecutionVertex &v : execPlan.getExecutionGraph()->getAllVertex()) {
        OperatorPtr operators = v.ptr->getRootOperator();
        if (operators) {
          vector<string> ssources = create_serialized_sources(schema, v, execPlan);
          vector<string> sdestinations = create_serialized_sinks(query + sensor_type, schema, v, execPlan);
          string soperators = SerializationTools::ser_operator(operators);
          string sschema = SerializationTools::ser_schema(schema);

          FogTopologyEntryPtr fogNode = v.ptr->getFogNode();
          auto hdl = actor_cast<actor>(this->state.topologyActorMap.at(fogNode));

          aout(this) << "Sending query to " << fogNode->getEntryTypeString() << to_string(hdl) << endl;
          this->request(hdl, task_timeout, execute_query_atom::value, query + sensor_type, sschema,
                        ssources, sdestinations, soperators);
        }
      }

      tuple<Schema, FogExecutionPlan, QueryExecutionPlanPtr> t = std::make_tuple(schema, execPlan, nullptr);
      this->state.runningQueries.insert({query + sensor_type, t});
      this->state.registeredQueries.erase(query + sensor_type);
    } else if (this->state.runningQueries.find(query + sensor_type) != this->state.runningQueries.end()) {
      aout(this) << "Query is already running -> " << query << "-" << sensor_type << endl;
    } else {
      aout(this) << "Query is not registered -> " << query << "-" << sensor_type << endl;
    }
  }

  void execute_query(const string &query, string &str_schema, vector<string> &str_sources,
                     vector<string> &str_destinations, string &str_ops) {
    aout(this) << "*** Executing query " << query << endl;
    Schema schema = SerializationTools::parse_schema(str_schema);
    DataSourcePtr source;
    DataSinkPtr sink;

    CodeGeneratorPtr code_gen = createCodeGenerator();
    PipelineContextPtr context = createPipelineContext();

    // Parse operators
    OperatorPtr oprtr = SerializationTools::parse_operator(str_ops);
    oprtr->produce(code_gen, context, std::cout);
    PipelineStagePtr stage = code_gen->compile(CompilerArgs());

    // Parse sources
    if (!str_sources.empty()) {
      if (str_sources[0] == "example") {
        // create example source
        source = createTestDataSourceWithSchema(schema);
        source->setNumBuffersToProcess(10);
      } else {
        aout(this) << "Query can not be executed! Only 'example' is supported -> " << query << endl;
        //source = SerializationTools::parse_source(str_sources[0]);
      }
    } else {
      // if there are no local input source operators use local zmq as source
      source = createZmqSource(schema, this->state.ip, get_assigned_port(query));
    }

    // Parse destinations
    if (!str_destinations.empty()) {
      sink = SerializationTools::parse_sink(str_destinations[0]);
    } else {
      sink = createPrintSinkWithSink(schema, std::cout);
    }

    QueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(nullptr, &stage));
    qep->addDataSource(source);
    qep->addDataSink(sink);

    std::cout << source->toString() << std::endl;
    std::cout << sink->toString() << std::endl;

    this->state.enginePtr->deployQuery(qep);
    get<2>(this->state.runningQueries.at(query)) = qep;

    if (source->isRunning()) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
    }
  }

  void delete_query(const string &query, const string &sensor_type) {
    if (this->state.registeredQueries.find(query + sensor_type) == this->state.registeredQueries.end() &&
        this->state.runningQueries.find(query + sensor_type) == this->state.runningQueries.end()) {
      aout(this) << "*** No deletion required! Query has neither been registered or deployed->" << query
                 << "-" << sensor_type << endl;
    } else if (this->state.registeredQueries.find(query + sensor_type) != this->state.registeredQueries.end()) {
      // Query is registered, but not running -> just remove from registered queries
      get<1>(this->state.registeredQueries.at(query + sensor_type)).freeResources();
      this->state.registeredQueries.erase(query + sensor_type);
      aout(this) << "Query was registered and has been succesfully removed -> " << query << "-" << sensor_type
                 << endl;
    } else {
      aout(this) << "Deleting..." << endl;
      try {
        //Query is running -> stop query at every worker where it is running and delete locally
        QueryExecutionPlanPtr qep = get<2>(this->state.runningQueries.at(query + sensor_type));
        get<1>(this->state.runningQueries.at(query + sensor_type)).freeResources();
        get<2>(this->state.runningQueries.at(query + sensor_type)).reset();

        if (qep) {
          this->state.enginePtr->undeployQuery(qep);
          aout(this) << "Query was running and has been succesfully removed -> " << query << "-"
                     << sensor_type << endl;
        } else {
          aout(this)
              << "Query was not running locally and deletion has been propagated to connected nodes -> "
              << query << "-" << sensor_type << endl;
        }
      }
      catch (...) {
        aout(this) << "Caught exception during deletion!";
      }

      for (auto const &x : this->state.actorTopologyMap) {
        strong_actor_ptr sap = x.first;
        FogTopologyEntryPtr e = x.second;

        if (e != this->state._thisEntry) {
          auto hdl = actor_cast<actor>(sap);
          this->request(hdl, task_timeout, delete_query_atom::value, query + sensor_type);
        }
      }
      //here it fails
      this->state.runningQueries.erase(query + sensor_type);
    }
    aout(this) << "*** successfully deleted query " << query << "-" << sensor_type << endl;
  }

  static vector<string>
  create_serialized_sources(const Schema &schema, const ExecutionVertex &v, const FogExecutionPlan &execPlan) {
    vector<string> output;

    if (execPlan.getExecutionGraph()->getAllEdgesToNode(v.ptr).empty()) {
      //TODO: check for local source operators, for now only example source generator is supported
      output.emplace_back("example");
    } else {
      // return empty list, cause it shall use local zmq
    }
    return output;
  }

  vector<string> create_serialized_sinks(const string &query, Schema &schema, const ExecutionVertex &v,
                                         const FogExecutionPlan &execPlan) {
    vector<string> output;
    for (const ExecutionNodePtr &_n: execPlan.getExecutionGraph()->getAllDestinationsFromNode(v.ptr)) {
      FogTopologyEntryPtr entry = _n->getFogNode();
      DataSinkPtr sink = createZmqSink(schema, entry->getIp(), get_assigned_port(query));
      output.emplace_back(SerializationTools::ser_sink(sink));
    }
    return output;
  }

  int get_assigned_port(const string &query) {
    OperatorPtr o;
    if (this->state.queryToPort.find(query) != this->state.queryToPort.end()) {
      return this->state.queryToPort.at(query);
    } else {
      // increase max port in map by 1
      this->state.receive_port += 1;
      this->state.queryToPort.insert({query, this->state.receive_port});
      return this->state.receive_port;
    }
  }
};
}

class coordinator_config : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t publish_port = 4711;
  uint16_t receive_port = 4815;

  coordinator_config() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port");
  }
};

void run_coordinator(actor_system &system, const coordinator_config &cfg) {
  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
         << "  quit                             : terminates the program" << endl
         << "  show topology                    : prints the topology" << endl
         << "  show registered                  : prints the registered queries" << endl
         << "  show running                     : prints the running queries" << endl
         << "  show operators                   : prints the deployed operators for all connected devices" << endl
         << "  register example <source>        : registers an example query" << endl
         << "  deploy example <source>          : executes the example query with BottomUp strategy" << endl
         << "  delete example <source>          : deletes the example query" << endl
         << endl;
  };
  usage();
  bool done = false;
  auto coord = system.spawn<iotdb::coordinator>(cfg.ip, cfg.publish_port, cfg.receive_port);
  // try to publish actor at given port
  cout << "*** try publish at port " << cfg.publish_port << endl;
  auto expected_port = io::publish(coord, cfg.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
              << system.render(expected_port.error()) << endl;
    return;
  }
  cout << "*** coordinator successfully published at port " << *expected_port << endl;

  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval{
      [&](const string &cmd) {
        if (cmd != "quit") {
          cout << "Unknown command" << endl;
          return;
        }
        anon_send_exit(coord, exit_reason::user_shutdown);
        done = true;
      },
      [&](string &arg0, string &arg1) {
        if (arg0 == "show" && arg1 == "topology") {
          anon_send(coord, topology_json_atom::value);
        } else if (arg0 == "show" && arg1 == "running") {
          anon_send(coord, show_running_atom::value);
        } else if (arg0 == "show" && arg1 == "registered") {
          anon_send(coord, show_registered_atom::value);
        } else if (arg0 == "show" && arg1 == "operators") {
          anon_send(coord, show_running_operators_atom::value);
        } else {
          cout << "Unknown command" << endl;
        }
      },
      [&](string &arg0, string &arg1, string &arg2) {
        if (arg0 == "register" && arg1 == "example") {
          anon_send(coord, register_query_atom::value, arg1, arg2, "BottomUp");
        } else if (arg0 == "deploy" && arg1 == "example") {
          anon_send(coord, deploy_query_atom::value, arg1, arg2);
        } else if (arg0 == "delete" && arg1 == "example") {
          anon_send(coord, delete_query_atom::value, arg1, arg2);
        } else {
          cout << "Unknown command" << endl;
        }
      }
  };
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    line = trim(std::move(line)); // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
      usage();
  }
}

void setupLogging() {
  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(new log4cxx::PatternLayout("%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  iotdb::logger->setLevel(log4cxx::Level::getDebug());
  iotdb::logger->addAppender(file);
  iotdb::logger->addAppender(console);
}

void caf_main(actor_system &system, const coordinator_config &cfg) {
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
  setupLogging();
  run_coordinator(system, cfg);
}

CAF_MAIN(io::middleman)