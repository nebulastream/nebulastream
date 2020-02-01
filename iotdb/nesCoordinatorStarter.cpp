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

namespace po = boost::program_options;
using namespace NES;

int main(int argc, const char *argv[]) {

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


  NesCoordinator *coord = new NesCoordinator();
  if(changed)
  {
    coord->setRestConfiguration(host, port);
  }
  coord->startCoordinator();
  coord->startCLI();

}

