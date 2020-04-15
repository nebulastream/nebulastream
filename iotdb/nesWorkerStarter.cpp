/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Worker
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/

#include <iostream>
#include <Util/UtilityFunctions.hpp>
#include "boost/program_options.hpp"
#include <Util/Logger.hpp>
#include <Components/NesWorker.hpp>

using std::cout;
using std::endl;
using std::string;
namespace po = boost::program_options;

using namespace caf;
using namespace NES;

const string logo = "/********************************************************\n"
                    " *     _   _   ______    _____\n"
                    " *    | \\ | | |  ____|  / ____|\n"
                    " *    |  \\| | | |__    | (___\n"
                    " *    | . ` | |  __|    \\___ \\     Worker\n"
                    " *    | |\\  | | |____   ____) |\n"
                    " *    |_| \\_| |______| |_____/\n"
                    " *\n"
                    " ********************************************************/";

int main(int argc, char** argv) {
    NES::setupLogging("nesWorkerStarter.log", NES::LOG_DEBUG);
    cout << logo << endl;
    namespace po = boost::program_options;
    po::options_description desc("Nes Worker Options");
    uint16_t actorPort = 0;
    std::string serverIp = "localhost";

    std::string sourceType = "";
    std::string sourceConfig = "";
    size_t sourceFrequency = 0;
    size_t numberOfBuffersToProduce = 1;
    std::string physicalStreamName;
    std::string logicalStreamName;
    std::string parentId = "-1";

    desc.add_options()("actor_port", po::value<uint16_t>(&actorPort)->default_value(actorPort),
                       "Set NES actor server port (default: 0).")
        ("server_ip", po::value<string>(&serverIp)->default_value(serverIp),
         "Set NES server ip (default: localhost).")
        (
            "sourceType", po::value<string>(&sourceType)->default_value(sourceType),
            "Set the type of the Source either CSVSource or DefaultSource")(
        "sourceConfig",
        po::value<string>(&sourceConfig)->default_value(sourceConfig),
        "Set the config for the source e.g. the file name")(
        "sourceFrequency",
        po::value<size_t>(&sourceFrequency)->default_value(sourceFrequency),
        "Set the sampling frequency")(
        "physicalStreamName",
        po::value<string>(&physicalStreamName)->default_value(physicalStreamName),
        "Set the physical name of the stream")(
        "numberOfBuffersToProduce",
        po::value<size_t>(&numberOfBuffersToProduce)->default_value(
            numberOfBuffersToProduce),
        "Set the number of buffers to produce")(
        "logicalStreamName",
        po::value<string>(&logicalStreamName)->default_value(logicalStreamName),
        "Set the logical stream name where this stream is added to")
        ("parentId",
         po::value<string>(&parentId)->default_value(parentId),
         "Set the parentId of this node")
        ("help", "Display help message");

    po::variables_map vm;

    try {
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Failure while parsing connection parameters!" << std::endl;
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    if (vm.count("help")) {
        std::cout << "Basic Command Line Parameter " << std::endl << desc
                  << std::endl;
        return 0;
    }

    if (argc == 1) {
        cout << "Please specify at least the port you want to connect to" <<
             endl;
        std::cout << "Basic Command Line Parameter " << std::endl << desc
                  << std::endl;
        return 0;
    }

    NesWorkerPtr wrk = std::make_shared<NesWorker>();

    //register phy stream if nessesary
    if (sourceType != "") {
        cout << "start with dedicated source=" << sourceType <<
             endl;
        PhysicalStreamConfig conf;
        conf.
            sourceType = sourceType;
        conf.
            sourceConfig = sourceConfig;
        conf.
            sourceFrequency = sourceFrequency;
        conf.
            numberOfBuffersToProduce = numberOfBuffersToProduce;
        conf.
            physicalStreamName = physicalStreamName;
        conf.
            logicalStreamName = logicalStreamName;

        wrk->
            setWitRegister(conf);

    } else if (parentId != "-1") {
        cout << "start with dedicated parent=" << parentId <<
             endl;
        wrk->
            setWithParent(parentId);
    }

    cout << "start with port=" << actorPort <<
         endl;
    wrk->start(/**blocking*/true, /**withConnect*/true, actorPort, serverIp);
    cout << "worker started" <<
         endl;

}
