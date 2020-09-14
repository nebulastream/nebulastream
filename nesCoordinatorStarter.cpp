/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Coordinator
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/
#include <Components/NesCoordinator.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <iostream>

namespace po = boost::program_options;
using namespace NES;
using namespace std;

const std::string logo = "/********************************************************\n"
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
    NES_INFO(logo);

    // Initializing defaults
    uint16_t restPort = 8081;
    uint16_t rpcPort = 4000;
    std::string serverIp = "127.0.0.1";
    uint16_t numberOfCpus = UINT16_MAX;

    po::options_description serverOptions("Nes Coordinator Server Options");
    serverOptions.add_options()(
        "serverIp", po::value<std::string>(&serverIp)->default_value(serverIp),
        "Set NES server ip (default: 127.0.0.1).")(
        "restPort", po::value<uint16_t>(),
        "Set NES REST server port (default: 8081).")

        (
            "coordinatorPort", po::value<uint16_t>(&rpcPort)->default_value(rpcPort),
            "Set NES rpc server port (default: 4000).")

        ("numberOfCpus", po::value<uint16_t>(&numberOfCpus)->default_value(numberOfCpus),
             "Set the computing capacity (default UINT16_MAX).")("help", "Display help message");

    /* Parse parameters. */
    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).options(serverOptions).run(), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        NES_ERROR("Failure while parsing connection parameters!");
        NES_ERROR(e.what());
        return EXIT_FAILURE;
    }

    if (vm.count("help")) {
        NES_INFO("Basic Command Line Parameter ");
        NES_INFO(serverOptions);
        return 0;
    }

    bool changed = false;
    if (vm.count("serverIp")) {
        changed = true;
    }

    if (vm.count("rest_port")) {
        changed = true;
    }

    NES_INFO("creating coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(serverIp, restPort, rpcPort, numberOfCpus);

    if (changed) {
        NES_INFO("config changed thus rest params");
    }
    if (serverIp != "127.0.0.1") {
        NES_INFO("set server ip to " << serverIp);
        crd->setServerIp(serverIp);
    }

    NES_INFO("start coordinator ip=" << serverIp << " with rpc port " << rpcPort << " restPort=" << restPort << " computingCapacity=" << numberOfCpus);
    crd->startCoordinator(/**blocking**/ true);//blocking call
    crd->stopCoordinator(true);
    NES_INFO("coordinator started");
}
