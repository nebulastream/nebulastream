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
#include <thread>
#include <caf/all.hpp>
#include <caf/io/all.hpp>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>

#include "REST/RestServer.hpp"
#include "Actors/ActorCoordinator.hpp"
#include "Actors/Configurations/ActorCoordinatorConfig.hpp"
#include "Actors/CAFServer.hpp"

using namespace iotdb;
using namespace caf;

namespace po = boost::program_options;
const uint16_t REST_PORT = 8081;

void startRestServer(std::string host, u_int16_t port) {
  RestServer restServer;
  restServer.start(host, port);
}

//TODO: pass the configuration using CLI or using a property file
void startCAFServer() {
  //Prepare configuration
  ActorCoordinatorConfig actorCoordinatorConfig;
  actorCoordinatorConfig.load<io::middleman>();

  //Prepare Actor System
  actor_system actorSystem{actorCoordinatorConfig};

  //Start CAF server
  CAFServer cafServer;
  cafServer.start(actorSystem, actorCoordinatorConfig);
}

int main(int argc, const char* argv[]) {

  // Initializing defaults
  bool startRest = true;
  std::string host = "localhost";
  uint16_t port = REST_PORT;

  po::options_description serverOptions("Coordinator Server Options");
  serverOptions.add_options()
      ("host", po::value<std::string>(), "Set NES Coordinator server host address (default: localhost).")
      ("start_rest_server", po::value<bool>(), "Start NES REST server (default: true).")
      ("rest_port", po::value<uint16_t>(), "Set NES REST server port (default: 8081).");

  /* All program options for command line. */
  po::options_description commandlineOptions;
  commandlineOptions.add(serverOptions);

  /* Parse parameters. */
  po::variables_map vm;
  try {
    po::store(po::parse_command_line(argc, argv, commandlineOptions), vm);
    po::notify(vm);
  }
  catch (const std::exception& e) {
    std::cerr << "Failure while parsing connection parameters!" << std::endl;
    std::cerr << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  if (vm.count("start_rest_server")) {
    startRest = vm["start_rest_server"].as<bool>();
  }

  if (vm.count("rest_host")) {
    host = vm["rest_host"].as<std::string>();
  }

  if (vm.count("rest_port")) {
    port = vm["rest_port"].as<uint16_t>();
  }

  //Start the CAF server
  std::thread t2(startCAFServer);
  std::cout << "CAF server started\n";

  //Start the rest server
  if (startRest) {
    std::thread t1(startRestServer, host, port);
    std::cout << "REST server started\n";
    t1.join();
  }

  t2.join();
}

