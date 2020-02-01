#include <Components/NesCoordinator.hpp>
#include <Util/Logger.hpp>
#include <caf/io/all.hpp>
#include <thread>

namespace NES {

NesCoordinator::NesCoordinator() {
  restPort = 8081;
  restHost = "localhost";

  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem { actorCoordinatorConfig };

  coordinatorActorHandle = actorSystem.spawn<CoordinatorActor>();
//
//  //Start the CAF server
//  std::thread t2(startCAFServer, coord);
//  std::cout << "CAF server started\n";
//
//  std::thread t1(startRestServer, restPort, restHost, coord);
//  std::cout << "REST server started\n";
//  t1.join();
//
//  t2.join();
}

bool NesCoordinator::startCoordinator() {
  NES_DEBUG("Start Rest Server")
  restServer = new RestServer(restHost, restPort, coordinatorActorHandle);
  bool retRest = restServer->start();
  if (retRest == true) {
    NES_DEBUG("Rest Server started successfully")
  } else {
    NES_DEBUG("Rest Server could not be started")
    return false;
  }
  cafServer = new CAFServer(coordinatorActorHandle);
  cafServer->start();

}

void NesCoordinator::setRestConfiguration(std::string host, uint16_t port) {
  this->restHost = restHost;
  this->restPort = port;
}

void NesCoordinator::startCLI() {
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

  message_handler eval { [&](string &arg0) {
    if (arg0 == "quit") {
      done = true;
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
}
