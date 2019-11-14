#include <iostream>
#include <Runtime/DataSink.hpp>
#include <Runtime/DataSource.hpp>
#include <Network/atom_utils.h>
#include <Runtime/CompiledDummyPlan.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/YSBWindow.hpp>
#include <Core/TupleBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <zmq.hpp>
#include <Runtime/ZmqSink.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>
#include <Util/SerializationTools.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>

#include <CodeGen/C_CodeGen/CodeCompiler.hpp>
#include <Util/ErrorHandling.hpp>
#include <CodeGen/QueryPlanBuilder.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/CodeGen.hpp>
#include <Operators/Impl/SourceOperator.hpp>

#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/device/back_inserter.hpp>
#include <boost/iostreams/stream.hpp>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <boost/serialization/access.hpp>

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

using namespace caf;
using namespace iotdb;

namespace iotdb {

struct __attribute__((packed)) ysbRecord {
  char user_id[16];
  char page_id[16];
  char campaign_id[16];
  char ad_type[9];
  char event_type[9];
  int64_t current_ms;
  uint32_t ip;

  ysbRecord() {
    event_type[0] = '-'; // invalid record
    current_ms = 0;
    ip = 0;
  }

  ysbRecord(const ysbRecord &rhs) {
    memcpy(&user_id, &rhs.user_id, 16);
    memcpy(&page_id, &rhs.page_id, 16);
    memcpy(&campaign_id, &rhs.campaign_id, 16);
    memcpy(&ad_type, &rhs.ad_type, 9);
    memcpy(&event_type, &rhs.event_type, 9);
    current_ms = rhs.current_ms;
    ip = rhs.ip;
  }
}; // size 78 bytes

class CompiledTestQueryExecutionPlan : public GeneratedQueryExecutionPlan {
 public:
  uint64_t count;
  uint64_t sum;

  CompiledTestQueryExecutionPlan() : GeneratedQueryExecutionPlan(), count(0), sum(0) {}

  void setDataSource(DataSourcePtr source) { sources.push_back(source); }

  bool firstPipelineStage(const TupleBuffer &) { return false; }

  bool executeStage(uint32_t pipeline_stage_id, const TupleBufferPtr buf) {
    DataSinkPtr sink = this->getSinks()[0];
    ysbRecord *record = (ysbRecord *) buf->buffer;

    //std::cout << "record.ad_type: " << record->ad_type << ", record.event_type: " << record->event_type << std::endl;

    TupleBufferPtr outputBuffer = BufferManager::instance().getBuffer();

    outputBuffer->buffer = record;

    outputBuffer->num_tuples = 1;
    outputBuffer->tuple_size_bytes = sizeof(ysbRecord);

    sink->writeData(outputBuffer);
    return true;
  }
};

typedef std::shared_ptr<CompiledTestQueryExecutionPlan> CompiledTestQueryExecutionPlanPtr;

constexpr auto task_timeout = std::chrono::seconds(10);

// the client queues pending tasks
struct sensor_state {
  strong_actor_ptr current_server;
  string sensor_type = "undefined";
  std::vector<string> tasks;
};

// class-based, statically typed, event-based API
class sensor : public stateful_actor<sensor_state> {
 public:
  explicit sensor(actor_config &cfg) : stateful_actor(cfg) {
    // nop
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
        },
        [=](set_type_atom, const std::string &type) {
          this->state.sensor_type = type;
        },
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
    this->request(coordinator, task_timeout, register_sensor_atom::value, 1, this_actor_ptr);

    this->state.tasks.clear();
    return {
        [=](connect_atom, const std::string &host, uint16_t port) {
          connecting(host, port);
        },
        [=](get_type_atom) {
          return this->state.sensor_type;
        }
    };
  }
};
}

class sensor_config : public actor_system_config {
 public:
  std::string type = "temperature";
  uint16_t port = 4711;
  std::string host = "localhost";

  sensor_config() {
    opt_group{custom_options_, "global"}
        .add(type, "type,t", "set type")
        .add(port, "port,p", "set port")
        .add(host, "host,H", "set host (ignored in server mode)");
  }
};

void start_sensor(actor_system &system, const sensor_config &cfg) {
  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
         << "  quit                  : terminates the program" << endl
         << "  connect <host> <port> : connects to a remote actor" << endl
         << endl;
  };
  usage();
  bool done = false;
  auto client = system.spawn<iotdb::sensor>();
  anon_send(client, set_type_atom::value, cfg.type);

  if (!cfg.host.empty() && cfg.port > 0)
    anon_send(client, connect_atom::value, cfg.host, cfg.port);
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
  // logger->setLevel(log4cxx::Level::getTrace());
  //	logger->setLevel(log4cxx::Level::getDebug());
  _logger->setLevel(log4cxx::Level::getInfo());
  //	logger->setLevel(log4cxx::Level::getWarn());
  // logger->setLevel(log4cxx::Level::getError());
  //	logger->setLevel(log4cxx::Level::getFatal());

  // add appenders and other will inherit the settings
  _logger->addAppender(file);
  _logger->addAppender(console);
}

void caf_main(actor_system &system, const sensor_config &cfg) {
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getInfo());
  setupLogging();

  //start_sensor(system, cfg);

  Schema schema = Schema::create()
      .addField("id", BasicType::UINT32)
      .addField("value", BasicType::UINT64);

  Stream cars = Stream("cars", schema);

  InputQuery &query = InputQuery::from(cars)
      .filter(cars["value"] > 42)
      .print(std::cout);

  CodeGeneratorPtr code_gen = createCodeGenerator();
  PipelineContextPtr context = createPipelineContext();

  OperatorPtr queryOp = query.getRoot();

  std::string s;
  {
    namespace io = boost::iostreams;
    io::stream<io::back_insert_device<std::string>> os(s);

    boost::archive::text_oarchive archive(os);
    archive << queryOp;
  }

  std::cout << "Deserializing: " << std::endl;
  OperatorPtr queryOpdeser;
  {
    namespace io = boost::iostreams;
    io::stream<io::array_source> is(io::array_source{s.data(), s.size()});
    boost::archive::text_iarchive archive(is);
    archive >> queryOpdeser;
  }

  queryOpdeser->produce(code_gen, context, std::cout);
  PipelineStagePtr stage = code_gen->compile(CompilerArgs());

  GeneratedQueryExecutionPlanPtr qep(new GeneratedQueryExecutionPlan(nullptr, &stage));

  // Create new Source and Sink
  //DataSourcePtr source = createCSVFileSource(schema, "tests/test_data/ysb-tuples-100-campaign-100.csv");
  DataSourcePtr source = createSchemaTestDataSource(schema);
  source->setNumBuffersToProcess(10);
  qep->addDataSource(source);

  //DataSinkPtr sink = createPrintSink(schema, cout);
  DataSinkPtr sink = createZmqSink(schema, "127.0.0.1", 4712);
  std::string ssink = SerializationTools::ser_sink(sink);
  DataSinkPtr deserSink = SerializationTools::parse_sink(ssink);
  std::cout << deserSink->toString() << std::endl;
  qep->addDataSink(deserSink);

  // start new query
  Dispatcher::instance().registerQuery(qep);
  ThreadPool::instance().start(1);

  struct __attribute__((packed)) ResultTuple {
    uint32_t id;
    uint64_t value;
  };

  // Start thread for receivingh the data.
  DataSourcePtr zmqSrc = createZmqSource(schema, "127.0.0.1", 4712);
  auto receiving_thread = std::thread([&]() {
    int i = 10;
    while (i <= 15) {
      i = i + 1;
      // Receive data.
      auto new_data = zmqSrc->receiveData();

      // Test received data.
      ResultTuple *tuple = (ResultTuple *) new_data->buffer;

      std::cout << "Result-> ID: " << tuple->id << " Value: " << tuple->value << std::endl;
    }
  });

  while (source->isRunning()) {
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }
  receiving_thread.join();
  qep->print();
}

CAF_MAIN(io::middleman)
