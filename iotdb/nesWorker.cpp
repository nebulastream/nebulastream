/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Worker
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/

#include <iostream>

#include <Actors/WorkerActor.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Actors/Configurations/WorkerActorConfig.hpp>

#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "boost/program_options.hpp"

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Actors/AtomUtils.hpp>
#include <Util/Logger.hpp>

using std::cout;
using std::endl;
using std::string;
namespace po = boost::program_options;

using namespace caf;
using namespace iotdb;

#define DEFAULT_NUMBER_OF_WORKER_INSTANCES 1

/**
 * @brief main method to run the worker with an interactive console command list
 */
size_t numberOfWorker = DEFAULT_NUMBER_OF_WORKER_INSTANCES;

void start_worker(actor_system &system, const WorkerActorConfig &cfg,
                  size_t numberOfWorker) {
  auto usage =
      [] {
        cout << "Usage:" << endl
        << "  quit                  : terminates the program" << endl
        << "  connect <host> <port> : connects to a remote actor" << endl
        << "  register_sensor "
        "<sourceType (OneGeneratorSource||CSVSource)> " "<sourceConf (1..n|filePath)> "
        "<physical stream name> " "<logical stream name> " ":register a physical stream"
        << "  register_stream " "<logical stream name> " "<pathToSchemaFile> " ": register a logical stream"
        << endl;
      };
  usage();

//  Test Code

//  getSchemaFromCode
//  std::string testSchema =
//      "Schema schema = Schema::create().addField(\"id\", BasicType::UINT32).addField(\"value\", BasicType::UINT64);";

//  SchemaPtr sch = UtilityFunctions::createSchemaFromCode(testSchema);
//  cout << "schema=" << sch->toString() << endl;

  infer_handle_from_class_t<iotdb::WorkerActor> client;

  PhysicalStreamConfig defaultConf;

  for (size_t i = 1; i <= numberOfWorker; i++) {
    client = system.spawn<iotdb::WorkerActor>(cfg.ip, cfg.publish_port,
                                              cfg.receive_port);
    if (!cfg.host.empty() && cfg.publish_port > 0) {
      //send connect message to worker to try to connect
      anon_send(client, connect_atom::value, cfg.host, cfg.publish_port);
    } else
      cout
          << "*** no server received via config, "
          << R"(please use "connect <host> <port>" before using the calculator)"
          << endl;
  }
  bool done = false;
  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler
eval(
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
      else if (arg0 == "register_stream") {
        cout << "NESWorker: register logical stream: stream name=" << arg1 << " filePath=" << arg2;
        anon_send(client, register_log_stream_atom::value, arg1, arg2);

      }

      },
      [&](string &arg0, string &arg1, string &arg2, string &arg3, string &arg4) {
        if (arg0 == "register_sesnor") {
          cout << "NESWorker: register physical stream: source type="
          << arg1 << " sourceConf=" << arg2
          << " physical stream name=" << arg3
          << " logical stream name=" << arg4 << endl;
        }
          anon_send(client, register_phy_stream_atom::value, arg1, arg2, arg3, arg4);
      }
  );
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    line = iotdb::UtilityFunctions::trim(std::move(line));  // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
    usage();
  }
}

static void setupLogging() {
  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  // set log level
  iotdb::iotdbLogger->setLevel(log4cxx::Level::getDebug());
  // add appenders and other will inherit the settings
  iotdb::iotdbLogger->addAppender(file);
  iotdb::iotdbLogger->addAppender(console);
}

void caf_main(actor_system &system, WorkerActorConfig &cfg,
    size_t numberOfWorker) {
//  setupLogging();
//  cout << argc << endl;
  start_worker(system, cfg, numberOfWorker);
}

int main(int argc, char** argv) {
  namespace po = boost::program_options;
  po::options_description desc("Options");
  size_t numberOfWorker = DEFAULT_NUMBER_OF_WORKER_INSTANCES;

  desc.add_options()("help", "Print help messages")(
      "numberOfWorker",
      po::value<size_t>(&numberOfWorker)->default_value(numberOfWorker),
      "The number of workers to spawn");

  po::variables_map vm;
  po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << "Basic Command Line Parameter " << std::endl << desc
    << std::endl;
    return 0;
  }
  std::cout << "Basic Command Line Parameter " << std::endl << desc
  << std::endl;

  std::cout << "Settings: " << "\n numberOfWorker: " << numberOfWorker
  << std::endl;

  WorkerActorConfig cfg;
  cfg.load<io::middleman>();
  actor_system system {cfg};

  caf_main(system, cfg, numberOfWorker);
}
