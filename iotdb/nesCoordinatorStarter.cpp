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

const string logo = "/********************************************************\n"
                    " *     _   _   ______    _____\n"
                    " *    | \\ | | |  ____|  / ____|\n"
                    " *    |  \\| | | |__    | (___\n"
                    " *    | . ` | |  __|    \\___ \\     Coordinator\n"
                    " *    | |\\  | | |____   ____) |\n"
                    " *    |_| \\_| |______| |_____/\n"
                    " *\n"
                    " ********************************************************/";

int main(int argc, const char* argv[]) {
    NES::setupLogging("nesCoordinatorStarter.log", NES::LOG_DEBUG);
    cout << logo << endl;

    // Initializing defaults
    uint16_t port = 8081;
    uint16_t actorPort = 0;
    std::string host = "localhost";
    std::string serverIp = "localhost";

    po::options_description serverOptions("Nes Coordinator Server Options");
    serverOptions.add_options()(
        "rest_host", po::value<std::string>(),
        "Set NES Coordinator server host address (default: localhost).")
        ("server_ip", po::value<string>(&serverIp)->default_value(serverIp),
         "Set NES server ip (default: localhost).")
        (
            "rest_port", po::value<uint16_t>(),
            "Set NES REST server port (default: 8081).")(
        "actor_port", po::value<uint16_t>(&actorPort)->default_value(actorPort),
        "Set NES actor server port (default: 0).")
        ("help", "Display help message");


    /* Parse parameters. */
    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).options(serverOptions).run(), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Failure while parsing connection parameters!" << std::endl;
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (vm.count("help")) {
        std::cout << "Basic Command Line Parameter " << std::endl << serverOptions
                  << std::endl;
        return 0;
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

    if (changed) {
        cout << "config changed thus rest params" << endl;
        crd->setRestConfiguration(host, port);
    }
    if (serverIp != "localhost") {
        cout << "set server ip to " << serverIp << endl;
        crd->setServerIp(serverIp);
    }

    cout << "start coordinator with port " << actorPort << endl;
    crd->startCoordinator(/**blocking**/true, actorPort);

    cout << "coordinator started" << endl;
}

