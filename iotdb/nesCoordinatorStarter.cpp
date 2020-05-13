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
#include <GRPC/CoordinatorRPCServer.hpp>
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

    CoordinatorRPCServer();
    //    RunServer();

    // Initializing defaults
    uint16_t restPort = 8081;
    uint16_t rpcPort = 4000;
    std::string serverIp = "localhost";

    po::options_description serverOptions("Nes Coordinator Server Options");
    serverOptions.add_options()(
        "serverIp", po::value<string>(&serverIp)->default_value(serverIp),
        "Set NES server ip (default: localhost).")
        (
            "restPort", po::value<uint16_t>(),
            "Set NES REST server port (default: 8081).")

        (
            "coordinatorPort", po::value<uint16_t>(&rpcPort)->default_value(rpcPort),
            "Set NES rpc server port (default: 4000).")
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
    if (vm.count("serverIp")) {
        changed = true;
    }

    if (vm.count("rest_port")) {
        changed = true;
    }

    cout << "creating coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(serverIp, restPort, rpcPort);

    if (changed) {
        cout << "config changed thus rest params" << endl;
    }
    if (serverIp != "localhost") {
        cout << "set server ip to " << serverIp << endl;
        crd->setServerIp(serverIp);
    }

    cout << "start coordinator ip=" << serverIp  << " with rpc port " << rpcPort << " restPort=" << restPort << endl;
    crd->startCoordinator(/**blocking**/true);
    cout << "coordinator started" << endl;

}

