#include <iostream>
#include <Network/atom_utils.hpp>
#include <utility>
#include <zmq.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>
#include <Util/SerializationTools.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/SourceCreator.hpp>
#include <Util/UtilityFunctions.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/SourceOperator.hpp>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

using std::cout;
using std::cerr;
using std::endl;
using std::string;

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::vector;
using std::tuple;
using std::unordered_map;

using namespace caf;
using namespace iotdb;

namespace iotdb {
constexpr auto task_timeout = std::chrono::seconds(10);

// the client queues pending tasks
struct worker_state {
  strong_actor_ptr current_server;
  string ip;
  uint16_t publish_port;
  uint16_t receive_port;
  string sensor_type;

  NodeEngine *enginePtr;

  std::unordered_map<string, tuple<QueryExecutionPlanPtr, OperatorPtr>> runningQueries;
};

// class-based, statically typed, event-based API
class worker : public stateful_actor<worker_state> {
 public:
  explicit worker(actor_config &cfg, string ip, uint16_t publish_port, uint16_t receive_port, string sensor_type)
      : stateful_actor(
      cfg) {
    this->state.ip = std::move(ip);
    this->state.publish_port = publish_port;
    this->state.receive_port = receive_port;
    this->state.sensor_type = std::move(sensor_type);
    this->state.enginePtr = new NodeEngine();
    this->state.enginePtr->start();
  }

  behavior_type make_behavior() override {
    return init();
  }

 private:
  // starting point of our FSM
  behavior init() {
    // transition to `unconnected` on server failure
    this->set_down_handler([=](const down_msg &dm) {
      if (dm.source == this->state.current_server) {
        aout(this) << "*** lost connection to server" << endl;
        this->state.current_server = nullptr;
        this->become(unconnected());
      }
    });
    return unconnected();
  }

  behavior unconnected() {
    return {
        [=](connect_atom, const std::string &host, uint16_t port) {
          connecting(host, port);
        }
    };
  }

  void connecting(const std::string &host, uint16_t port) {
    // make sure we are not pointing to an old server
    this->state.current_server = nullptr;
    // use request().await() to suspend regular behavior until MM responded
    auto mm = this->system().middleman().actor_handle();
    this->request(mm, infinite, connect_atom::value, host, port).await(
        [=](const node_id &, strong_actor_ptr &serv,
            const std::set<std::string> &ifs) {
          if (!serv) {
            aout(this) << R"(*** no server found at ")" << host << R"(":)"
                       << port << endl;
            return;
          }
          if (!ifs.empty()) {
            aout(this) << R"(*** typed actor found at ")" << host << R"(":)"
                       << port << ", but expected an untyped actor " << endl;
            return;
          }
          aout(this) << "*** successfully connected to server" << endl;
          this->state.current_server = serv;
          auto hdl = actor_cast<actor>(serv);
          this->monitor(hdl);
          this->become(running(hdl));
        },
        [=](const error &err) {
          aout(this) << R"(*** cannot connect to ")" << host << R"(":)"
                     << port << " => " << this->system().render(err) << endl;
          this->become(unconnected());
        }
    );
  }

  // prototype definition for transition to `running` with Calculator
  behavior running(const actor &coordinator) {
    auto this_actor_ptr = actor_cast<strong_actor_ptr>(this);
    //TODO: change me
    this->request(coordinator, task_timeout, register_sensor_atom::value, this->state.ip,
                  this->state.publish_port, this->state.receive_port, 2, this->state.sensor_type,
                  this_actor_ptr);
    //this->request(coordinator, task_timeout, register_worker_atom::value, 2, this_actor_ptr);

    return {
        [=](connect_atom, const std::string &host, uint16_t port) {
          connecting(host, port);
        },
        [=](execute_query_atom, const string &query, string &str_schema, vector<string> &str_sources,
            vector<string> &str_destinations, string &str_ops) {
          this->execute_query(query, str_schema, str_sources, str_destinations, str_ops);
        },
        [=](delete_query_atom, const string &query) {
          this->delete_query(query);
        },
        [=](get_operators_atom) {
          return this->getOperators();
        }
    };
  }

  vector<string> getOperators() {
    vector<string> result;
    for (auto const &x : this->state.runningQueries) {
      string str_opts;
      vector<OperatorType> flattened = get<1>(x.second)->flattenedTypes();
      for (const OperatorType &_o: flattened) {
        if (!str_opts.empty())
          str_opts.append(", ");
        str_opts.append(operatorTypeToString.at(_o));
      }
      result.emplace_back(x.first + "->" + str_opts);
    }
    return result;
  }

  void delete_query(const string &query) {
    auto sap = actor_cast<strong_actor_ptr>(this);
    if (this->state.runningQueries.find(query) != this->state.runningQueries.end()) {
      QueryExecutionPlanPtr qep = get<0>(this->state.runningQueries.at(query));
      get<0>(this->state.runningQueries.at(query)).reset();
      get<1>(this->state.runningQueries.at(query)).reset();
      this->state.enginePtr->undeployQuery(qep);
      this->state.runningQueries.erase(query);
      aout(this) << to_string(sap) << ": *** Successfully deleted query " << query << endl;
    } else {
      aout(this) << to_string(sap) << ": *** Query not found for deletion -> " << query << endl;
    }
  }

  void execute_query(const string &query, string &str_schema, vector<string> &str_sources,
                     vector<string> &str_destinations, string &str_ops) {

    if (this->state.runningQueries.find(query) == this->state.runningQueries.end()) {
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
        source = createZmqSource(schema, this->state.ip, this->state.receive_port);
      }

      // Parse destinations
      if (!str_destinations.empty()) {
        sink = SerializationTools::parse_sink(str_destinations[0]);
      } else {
        sink = createPrintSinkWithSink(schema, std::cout);
      }

      QueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan());
      qep->addDataSource(source);
      qep->addDataSink(sink);

      std::cout << source->toString() << std::endl;
      std::cout << sink->toString() << std::endl;

      this->state.enginePtr->deployQuery(qep);
      this->state.runningQueries.insert({query, std::make_tuple(qep, oprtr)});

      if (source->isRunning()) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
      }
    } else {
      aout(this) << "*** Query already running -> " << query << endl;
    }
  }

};
}

class worker_config : public actor_system_config {
 public:
  std::string ip = "127.0.0.1";
  uint16_t receive_port = 0;
  std::string host = "localhost";
  uint16_t publish_port = 4711;
  std::string sensor_type = "cars";

  worker_config() {
    opt_group{custom_options_, "global"}
        .add(ip, "ip", "set ip")
        .add(publish_port, "publish_port,ppub", "set publish_port")
        .add(receive_port, "receive_port,prec", "set receive_port")
        .add(host, "host,H", "set host (ignored in server mode)")
        .add(sensor_type, "sensor_type", "set sensor_type");
  }
};

void start_worker(actor_system &system, const worker_config &cfg) {
  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
         << "  quit                  : terminates the program" << endl
         << "  connect <host> <port> : connects to a remote actor" << endl
         << endl;
  };
  usage();

  infer_handle_from_class_t<iotdb::worker> client;

  for (int i = 1; i <= 5; i++) {
    client = system.spawn<iotdb::worker>(cfg.ip, cfg.publish_port,
                                         cfg.receive_port,
                                         cfg.sensor_type + std::to_string(i));
    if (!cfg.host.empty() && cfg.publish_port > 0)
      anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
    else
      cout << "*** no server received via config, "
           << R"(please use "connect <host> <port>" before using the calculator)"
           << endl;
  }

  bool done = false;
  client = system.spawn<iotdb::worker>(cfg.ip, cfg.publish_port, cfg.receive_port, cfg.sensor_type);
  if (!cfg.host.empty() && cfg.publish_port > 0)
    anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
  else
    cout << "*** no server received via config, "
         << R"(please use "connect <host> <port>" before using the calculator)"
         << endl;

  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval{
      [&](const string &cmd) {
        if (cmd != "quit")
          return;
        anon_send_exit(client, exit_reason::user_shutdown);
        done = true;
      },
      [&](string &arg0, string &arg1, string &arg2) {
        if (arg0 == "connect") {
          char *end = nullptr;
          auto lport = strtoul(arg2.c_str(), &end, 10);
          if (end != arg2.c_str() + arg2.size())
            cout << R"(")" << arg2 << R"(" is not an unsigned integer)" << endl;
          else if (lport > std::numeric_limits<uint16_t>::max())
            cout << R"(")" << arg2 << R"(" > )"
                 << std::numeric_limits<uint16_t>::max() << endl;
          else
            anon_send(client, connect_atom::value, move(arg1),
                      static_cast<uint16_t>(lport));
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

  // set log level
  iotdb::logger->setLevel(log4cxx::Level::getInfo());
  // add appenders and other will inherit the settings
  iotdb::logger->addAppender(file);
  iotdb::logger->addAppender(console);
}

void caf_main(actor_system &system, const worker_config &cfg) {
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
  setupLogging();
  start_worker(system, cfg);
}

CAF_MAIN(io::middleman)