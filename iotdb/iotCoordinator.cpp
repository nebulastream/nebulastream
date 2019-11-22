#include <iostream>
#include <Actors/atom_utils.hpp>
#include <Actors/NesCoordinator.hpp>
#include <Util/UtilityFunctions.hpp>

#include <string>
#include <utility>

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

// class-based, statically typed, event-based API for the state management in CAF
struct coordinator_state {
  std::shared_ptr<NesCoordinator> coordinatorPtr;
  unordered_map<strong_actor_ptr, FogTopologyEntryPtr> actorTopologyMap;
  unordered_map<FogTopologyEntryPtr, strong_actor_ptr> topologyActorMap;

  NodeEngine *enginePtr;
  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> runningQueries;
};

/**
   * @brief the coordinator for NES
   */
class coordinator : public stateful_actor<coordinator_state> {
 public:
  /**
   * @brief the constructior of the coordinator to initialize the default objects
   */
  explicit coordinator(actor_config &cfg, string ip, uint16_t publish_port, uint16_t receive_port)
      : stateful_actor(cfg) {
    this->state.coordinatorPtr = std::make_shared<NesCoordinator>(NesCoordinator(ip, publish_port, receive_port));

    this->state.actorTopologyMap.insert({this->address().get(), this->state.coordinatorPtr->getThisEntry()});
    this->state.topologyActorMap.insert({this->state.coordinatorPtr->getThisEntry(), this->address().get()});

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
        this->state.coordinatorPtr->deregister_sensor(this->state.actorTopologyMap.at(key));
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
        [=](register_sensor_atom, string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
            const string &sensor_type, const strong_actor_ptr &sap) {
          // rpc to register sensor
          this->register_sensor(ip, publish_port, receive_port, cpu, sensor_type, sap);
        },
        [=](register_query_atom, const string &description, const string &sensor_type, const string &strategy) {
          // rpc to register queries
          this->state.coordinatorPtr->register_query(description, sensor_type, strategy);
        },
        [=](delete_query_atom, const string &description) {
          // rpc to unregister a registered query
          this->delete_query(description);
        },
        [=](deploy_query_atom, const string &description) {
          // rpc to deploy queries to all corresponding actors
          this->deploy_query(description);
        },
        [=](execute_query_atom, const string &description, string &executableTransferObject) {
          // internal rpc to execute a query
          this->execute_query(description, executableTransferObject);
        },
        [=](get_operators_atom) {
          // internal rpc to return currently running operators on this node
          return this->state.coordinatorPtr->getOperators();
        },
        // external methods for users
        [=](topology_json_atom) {
          // print the topology
          string topo = this->state.coordinatorPtr->getTopologyPlanString();
          aout(this) << "Printing Topology" << endl;
          aout(this) << topo << endl;
        },
        [=](show_registered_atom) {
          // print registered queries
          aout(this) << "Printing Registered Queries" << endl;
          for (const auto &p : this->state.coordinatorPtr->getRegisteredQueries()) {
            aout(this) << p.first << endl;
          }
        },
        [=](show_running_atom) {
          // print running queries
          aout(this) << "Printing Running Queries" << endl;
          for (const auto &p : this->state.coordinatorPtr->getRunningQueries()) {
            aout(this) << p.first << endl;
          }
        },
        [=](show_running_operators_atom) {
          // print running operators in the whole topology
          aout(this) << "Requesting deployed operators from connected devices.." << endl;
          this->show_operators();
        }
    };
  }

  void register_sensor(const string &ip, uint16_t publish_port, uint16_t receive_port, int cpu,
                       const string &sensor_type, const strong_actor_ptr &sap) {
    auto hdl = actor_cast<actor>(sap);
    FogTopologyEntryPtr
        sensorNode = this->state.coordinatorPtr->register_sensor(ip, publish_port, receive_port, cpu, sensor_type);

    this->state.actorTopologyMap.insert({sap, sensorNode});
    this->state.topologyActorMap.insert({sensorNode, sap});
    this->monitor(hdl);
    aout(this) << "*** successfully registered sensor (CPU=" << cpu << ", Type: "
               << sensor_type << ") "
               << to_string(hdl) << endl;
  }

  void deploy_query(const string &description) {
    unordered_map<FogTopologyEntryPtr, ExecutableTransferObject>
        deployments = this->state.coordinatorPtr->make_deployment(description);

    for (auto const &x : deployments) {
      strong_actor_ptr sap = this->state.topologyActorMap.at(x.first);
      auto hdl = actor_cast<actor>(sap);
      string s_eto = SerializationTools::ser_eto(x.second);
      IOTDB_INFO("Sending query " << description << " to " << to_string(hdl));
      this->request(hdl, task_timeout, execute_query_atom::value, description, s_eto);
    }
  }

  /**
   * @brief framework internal method which is called to execute a query or sub-query on a node
   * @param description a description of the query
   * @param executableTransferObject wrapper object with the schema, sources, destinations, operator
   */
  void execute_query(const string &description, string &executableTransferObject) {
    ExecutableTransferObject eto = SerializationTools::parse_eto(executableTransferObject);
    QueryExecutionPlanPtr qep = eto.toQueryExecutionPlan();
    this->state.runningQueries.insert({description, std::make_tuple(qep, eto.getOperatorTree())});
    this->state.enginePtr->deployQuery(qep);
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  /**
   * @brief method which is called to unregister an already running query
   * @param description the description of the query
   */
  void delete_query(const string &description) {
    // send command to all corresponding nodes to stop the running query as well
    for (auto const &x : this->state.actorTopologyMap) {
      strong_actor_ptr sap = x.first;
      FogTopologyEntryPtr e = x.second;

      if (e != this->state.coordinatorPtr->getThisEntry()) {
        auto hdl = actor_cast<actor>(sap);
        this->request(hdl, task_timeout, delete_query_atom::value, description);
      }
    }

    if (this->state.coordinatorPtr->deregister_query(description)) {
      //Query is running -> stop query locally if it is running and free resources
      try {
        this->state.enginePtr->undeployQuery(get<0>(this->state.runningQueries.at(description)));
        std::this_thread::sleep_for(std::chrono::seconds(3));
        this->state.runningQueries.erase(description);
      }
      catch (...) {
        // TODO: catch ZMQ termination errors properly
        IOTDB_ERROR("Uncaugth error during deletion!")
      }
    }
  }

  /**
 * @brief send messages to all connected devices and get their operators
 */
  void show_operators() {
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
};
}

/**
   * @brief The configuration file of the coordinator
   */
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

/**
   * @brief main method to run the coordinator with an interactive console command list
   */
void run_coordinator(actor_system &system, const coordinator_config &cfg) {
  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
         << "  quit                                 : terminates the program" << endl
         << "  show topology                        : prints the topology" << endl
         << "  show registered                      : prints the registered queries" << endl
         << "  show running                         : prints the running queries" << endl
         << "  show operators                       : prints the deployed operators for all connected devices" << endl
         << "  register <description> <source>      : registers an example query" << endl
         << "  deploy <description>                 : executes the example query with BottomUp strategy" << endl
         << "  delete <description>                 : deletes the example query" << endl
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
        } else if (arg0 == "deploy" && !arg1.empty()) {
          anon_send(coord, deploy_query_atom::value, arg1);
        } else if (arg0 == "delete" && !arg1.empty()) {
          anon_send(coord, delete_query_atom::value, arg1);
        } else {
          cout << "Unknown command" << endl;
        }
      },
      [&](string &arg0, string &arg1, string &arg2) {
        if (arg0 == "register" && arg1 == "example" && !arg2.empty()) {
          anon_send(coord, register_query_atom::value, arg1, arg2, "BottomUp");
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

// the main method
void caf_main(actor_system &system, const coordinator_config &cfg) {
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
  setupLogging();
  run_coordinator(system, cfg);
}

CAF_MAIN(io::middleman)