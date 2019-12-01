#include <iostream>
#include <Actors/ActorCoordinator.hpp>

#include "caf/all.hpp"
#include "caf/io/all.hpp"

using std::cout;
using std::endl;
using std::string;

using namespace caf;
using namespace iotdb;

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
  auto coord = system.spawn<iotdb::actor_coordinator>(cfg.ip, cfg.publish_port, cfg.receive_port);
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
          anon_send(coord, deregister_query_atom::value, arg1);
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
    line = iotdb::trim(std::move(line)); // ignore leading and trailing whitespaces
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