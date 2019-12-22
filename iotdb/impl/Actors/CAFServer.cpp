#include <iostream>

#include "Actors/CAFServer.hpp"

using namespace iotdb;
using namespace caf;

bool CAFServer::start(
    const infer_handle_from_class_t<CoordinatorActor>& coordinatorActorHandle) {

  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem { actorCoordinatorConfig };
  //Setup then logging
  setupLogging();

  cout << "*** trying to publish at port "
       << actorCoordinatorConfig.publish_port << endl;

  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(coordinatorActorHandle,
                                   actorCoordinatorConfig.publish_port);
  if (!expected_port) {
    std::cerr << "*** publish failed: "
        << actorSystem.render(expected_port.error()) << endl;
    return false;
  }
  cout << "*** coordinator successfully published at port " << *expected_port
      << endl;

  //TODO: This code is to be migrated when we create CLI based client for interacting with the actor_system

  bool done = false;

  // keeps track of requests and tries to reconnect on server failures
  auto usage =
      [] {
        cout << "Usage:" << endl
        << "  quit                                 : terminates the program" << endl
        << "  show topology (st)                   : prints the topology" << endl
        << "  show deployed (sd)          : prints the deployed queries" << endl
        << "  show running (sr)                   : prints the running queries" << endl
        << "  show operators (so)                  : prints the deployed operators for all connected devices" << endl
        << "  register <description>               : registers an example query (only 'example' string can be supplied)" << endl
        << "  deploy <description>                 : executes the example query with BottomUp strategy" << endl
        << "  delete <description>                 : deletes the example query" << endl
        << endl;
      };
  usage();

  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval { [&](const string &cmd) {
    if (cmd != "quit") {
      cout << "Unknown command" << endl;
      return;
    }
    anon_send_exit(coordinatorActorHandle, exit_reason::user_shutdown);
    done = true;
  }, [&](string &arg0) {
    if ( arg0 == "st") {
      anon_send(coordinatorActorHandle, topology_json_atom::value);
    } else if (arg0 == "sd") {
      anon_send(coordinatorActorHandle, show_registered_atom::value);
    } else if (arg0 == "sr") {
      anon_send(coordinatorActorHandle, show_running_atom::value);
    } else if (arg0 == "so") {
      anon_send(coordinatorActorHandle, show_running_operators_atom::value);
    } else {
      cout << "Unknown command" << endl;
    }
  },
    [&](string &arg0, string &arg1) {
      if ((arg0 == "show" && arg1 == "topology") || arg0 == "st") {
        anon_send(coordinatorActorHandle, topology_json_atom::value);
      } else if ((arg0 == "show" && arg1 == "deployed") || arg0 == "sd") {
        anon_send(coordinatorActorHandle, show_registered_atom::value);
      } else if ((arg0 == "show" && arg1 == "running")|| arg0 == "sr") {
        anon_send(coordinatorActorHandle, show_running_atom::value);
      } else if ((arg0 == "show" && arg1 == "operators") || arg0 == "so") {
        anon_send(coordinatorActorHandle, show_running_operators_atom::value);
      } else if (arg0 == "deploy" && !arg1.empty()) {
        anon_send(coordinatorActorHandle, deploy_query_atom::value, arg1);
      } else if (arg0 == "delete" && !arg1.empty()) {
        anon_send(coordinatorActorHandle, deregister_query_atom::value, arg1);
      } else if (arg0 == "register") {
        anon_send(coordinatorActorHandle, register_query_atom::value, arg1, "BottomUp");
      } else {
        cout << "Unknown command" << endl;
      }
    },
    [&](string& arg0, string& arg1, string& arg2) {
      if (arg0 == "register") {
        anon_send(coordinatorActorHandle, register_query_atom::value, arg1, arg2);
      } else {
        cout << "Unknown command" << endl;
      }
    }
  };
  // read next line, split it, and feed to the eval handler
  string line;
  while (!done && std::getline(std::cin, line)) {
    line = iotdb::trim(std::move(line));  // ignore leading and trailing whitespaces
    std::vector<string> words;
    split(words, line, is_any_of(" "), token_compress_on);
    if (!message_builder(words.begin(), words.end()).apply(eval))
      usage();
  }
  return true;

}

void CAFServer::setupLogging() {

  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "iotdb.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  iotdb::logger->setLevel(log4cxx::Level::getDebug());
  iotdb::logger->addAppender(file);
  iotdb::logger->addAppender(console);

  // set log level
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
}

