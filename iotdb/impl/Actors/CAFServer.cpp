#include <iostream>

#include "Actors/CAFServer.hpp"
#include <Util/Logger.hpp>
using namespace NES;
using namespace caf;

bool CAFServer::start(
    const infer_handle_from_class_t<CoordinatorActor>& coordinatorActorHandle) {

  CoordinatorActorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem { actorCoordinatorConfig };
  //Setup then logging
  setupLogging();

  IOTDB_DEBUG("*** trying to publish at port "
       << actorCoordinatorConfig.publish_port)

  io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);

  auto expected_port = io::publish(coordinatorActorHandle,
                                   actorCoordinatorConfig.publish_port);
  if (!expected_port) {
    IOTDB_ERROR("*** publish failed: "
        << actorSystem.render(expected_port.error()))
    return false;
  }
  IOTDB_DEBUG("*** coordinator successfully published at port " << *expected_port)

  //TODO: This code is to be migrated when we create CLI based client for interacting with the actor_system

  bool done = false;

  // keeps track of requests and tries to reconnect on server failures
  auto usage =
      [] {
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

  // defining the handler outside the loop is more efficient as it avoids
  // re-creating the same object over and over again
  message_handler eval {
      [&](string &arg0) {
        if(arg0 == "quit")
        {
          done = true;
          anon_send_exit(coordinatorActorHandle, exit_reason::user_shutdown);
          io::unpublish(coordinatorActorHandle, actorCoordinatorConfig.publish_port);
          return true;
        }
        else if (arg0 == "st") {
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
          anon_send(coordinatorActorHandle, register_query_atom::value, arg1, "BottomUp");
        } else {
          cout << "Unknown command " << arg0 << "," << arg1 << endl;
        }
      },
      [&](string& arg0, string& arg1, string& arg2) {
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
      }
  };
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
  return true;

}

void CAFServer::setupLogging() {

  // create PatternLayout
  log4cxx::LayoutPtr layoutPtr(
      new log4cxx::PatternLayout(
          "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

  // create FileAppender
  LOG4CXX_DECODE_CHAR(fileName, "cafServer.log");
  log4cxx::FileAppenderPtr file(new log4cxx::FileAppender(layoutPtr, fileName));

  // create ConsoleAppender
  log4cxx::ConsoleAppenderPtr console(new log4cxx::ConsoleAppender(layoutPtr));

  NES::iotdbLogger->setLevel(log4cxx::Level::getDebug());
  NES::iotdbLogger->addAppender(file);
  NES::iotdbLogger->addAppender(console);

  // set log level
  log4cxx::Logger::getLogger("IOTDB")->setLevel(log4cxx::Level::getDebug());
}

