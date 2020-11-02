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
    std::cout << logo << std::endl;

    // Initializing defaults
    uint16_t restPort = 8081;
    uint16_t rpcPort = 4000;
    std::string serverIp = "127.0.0.1";
    std::string logLevel = "LOG_DEBUG";

    // set the default numberOfSlots to the number of processor
    const auto processorCount = std::thread::hardware_concurrency();
    uint16_t numberOfSlots = processorCount;
    bool enableQueryMerging = false;

    po::options_description serverOptions("Nes Coordinator Server Options");
    serverOptions.add_options()(
        "serverIp", po::value<std::string>(&serverIp)->default_value(serverIp), "Set NES server ip (default: 127.0.0.1).")(
        "restPort", po::value<uint16_t>(), "Set NES REST server port (default: 8081).")(
        "coordinatorPort", po::value<uint16_t>(&rpcPort)->default_value(rpcPort), "Set NES rpc server port (default: 4000).")(
        "numberOfSlots", po::value<uint16_t>(&numberOfSlots)->default_value(numberOfSlots), "Set the computing capacity (default: number of processor).")(
        "enableQueryMerging", po::value<bool>(&enableQueryMerging)->default_value(enableQueryMerging), "Enable Query Merging Feature (default: false).")(
        "logLevel", po::value<std::string>(&logLevel)->default_value(logLevel), "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)")(
        "help", "Display help message");

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
    NES::setupLogging("nesCoordinatorStarter.log", NES::getStringAsDebugLevel(logLevel));

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
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(serverIp, restPort, rpcPort, numberOfSlots, enableQueryMerging);

    if (changed) {
        NES_INFO("config changed thus rest params");
    }
    if (serverIp != "127.0.0.1") {
        NES_INFO("set server ip to " << serverIp);
        crd->setServerIp(serverIp);
    }

    NES_INFO("start coordinator ip=" << serverIp << " with rpc port " << rpcPort << " restPort=" << restPort << " numberOfSlots=" << numberOfSlots);
    crd->startCoordinator(/**blocking**/ true);//blocking call
    crd->stopCoordinator(true);
    NES_INFO("coordinator started");
}
