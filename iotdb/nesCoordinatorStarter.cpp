/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Coordinator
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <Components/NesCoordinator.hpp>
#include <caf/io/all.hpp>
#include <Actors/AtomUtils.hpp>
#include <Util/Logger.hpp>

namespace po = boost::program_options;
using namespace NES;

static void setupLogging() {
// create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

// create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "nesCoordinator.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

// create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

// set log level
  NES::NESLogger->setLevel(log4cxx::Level::getDebug());
// add appenders and other will inherit the settings
  NES::NESLogger->addAppender(file);
  NES::NESLogger->addAppender(console);
}

#if 0
void startCLI(NesCoordinatorPtr coordinatorActorHandle) {
  bool done = false;

  // keeps track of requests and tries to reconnect on server failures
  auto usage = [] {
    cout << "Usage:" << endl
    << "  quit                                 : terminates the program" << endl
    << "  show topology (st)                   : prints the topology" << endl
    << "  show registered queries (sregq)      : prints the registered queries" << endl
    << "  show running queries (srunq)         : prints the running queries" << endl
    << "  show logical streams (sls)           : prints the available logical streams" << endl
    << "  show physical streams (sph)          : prints the available physical streams" << endl
    << "  show running operators (sro)         : prints the deployed operators for all connected devices" << endl
    << "  register <description>               : registers an example query (only 'example' string can be supplied)" << endl
    << "  deploy <description>                 : executes the example query with BottomUp strategy" << endl
    << "  delete <description>                 : deletes the example query" << endl
    << endl;
  };
  usage();
  coordinatorActorHandle->
  message_handler eval { [&](string &arg0) {
    if (arg0 == "quit") {
      done = true;
      CoordinatorActorConfig actorCoordinatorConfig;
      anon_send_exit(coordinatorActorHandle, exit_reason::user_shutdown);
      io::unpublish(coordinatorActorHandle,
                    actorCoordinatorConfig.publish_port);
      return true;
    } else if (arg0 == "st") {
      anon_send(coordinatorActorHandle, topology_json_atom::value);
    } else if (arg0 == "sregq") {
      anon_send(coordinatorActorHandle, show_registered_queries_atom::value);
    } else if (arg0 == "srunq") {
      anon_send(coordinatorActorHandle, show_running_queries_atom::value);
    } else if (arg0 == "sro") {
      anon_send(coordinatorActorHandle, show_running_operators_atom::value);
    } else if (arg0 == "sls") {
      anon_send(coordinatorActorHandle, show_reg_log_stream_atom::value);
    } else if (arg0 == "sph") {
      anon_send(coordinatorActorHandle, show_reg_phy_stream_atom::value);
    } else {
      cout << "Unknown command " << arg0 << endl;
    }
  },
  [&](string &arg0, string &arg1) {
    if ((arg0 == "show" && arg1 == "topology")) {
      anon_send(coordinatorActorHandle, topology_json_atom::value);
    } else if (arg0 == "deploy" && !arg1.empty()) {
      anon_send(coordinatorActorHandle, deploy_query_atom::value, arg1);
    } else if (arg0 == "delete" && !arg1.empty()) {
      anon_send(coordinatorActorHandle, deregister_query_atom::value, arg1);
    } else if (arg0 == "register") {
      anon_send(coordinatorActorHandle, register_query_atom::value, arg1,
                "BottomUp");
    } else {
      cout << "Unknown command " << arg0 << "," << arg1 << endl;
    }
  },
  [&](string &arg0, string &arg1, string &arg2) {
    if (arg0 == "register") {
      anon_send(coordinatorActorHandle, register_query_atom::value, arg1, arg2);
    } else if ((arg0 == "show" && arg1 == "registered" && arg2 == "queries")) {
      anon_send(coordinatorActorHandle, show_registered_queries_atom::value);
    } else if ((arg0 == "show" && arg1 == "running" && arg2 == "queries")) {
      anon_send(coordinatorActorHandle, show_running_queries_atom::value);
    } else if ((arg0 == "show" && arg1 == "running" && arg2 == "operators")) {
      anon_send(coordinatorActorHandle, show_running_operators_atom::value);
    } else if ((arg0 == "show" && arg1 == "logical" && arg2 == "streams")) {
      anon_send(coordinatorActorHandle, show_reg_log_stream_atom::value);
    } else if ((arg0 == "show" && arg1 == "physical" && arg2 == "streams")) {
      anon_send(coordinatorActorHandle, show_reg_phy_stream_atom::value);
    } else {
      cout << "Unknown command " << arg0 << "," << arg1 << "," << arg2 << endl;
    }
  } };
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    cout << "line=" << line << " done=" << done << endl;
    line = NES::UtilityFunctions::trim(std::move(line));  // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
      usage();
  }
}
#endif
int main(int argc, const char *argv[]) {
  setupLogging();
  // Initializing defaults
  uint16_t port = 8081;
  std::string host = "localhost";

  po::options_description serverOptions("Coordinator Server Options");
  serverOptions.add_options()(
      "rest_host", po::value<std::string>(),
      "Set NES Coordinator server host address (default: localhost).")(
      "rest_port", po::value<uint16_t>(),
      "Set NES REST server port (default: 8081).");

  /* All program options for command line. */
  po::options_description commandlineOptions;
  commandlineOptions.add(serverOptions);

  /* Parse parameters. */
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, commandlineOptions), vm);
    po::notify(vm);
  } catch (const std::exception &e) {
    std::cerr << "Failure while parsing connection parameters!" << std::endl;
    std::cerr << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  bool changed = false;
  if (vm.count("rest_host")) {
    host = vm["rest_host"].as<std::string>();
    changed = true;
  }

  if (vm.count("rest_port")) {
    port = vm["rest_port"].as<uint16_t>();
    changed = true;
  }

  cout << "creating coordinator" << endl;

  NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();

  if(changed)
  {
    cout << "config changed thus rest params" << endl;
    crd->setRestConfiguration(host, port);
  }
  cout << "start coordinator" << endl;
  crd->startCoordinatorBlocking();

  cout << "coordinator started" << endl;

//  cout << "start CLI" << endl;
//  startCLI(crd);

}

