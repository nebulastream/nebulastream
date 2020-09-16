/********************************************************
 *     _   _   ______    _____
 *    | \ | | |  ____|  / ____|
 *    |  \| | | |__    | (___
 *    | . ` | |  __|    \___ \     Worker
 *    | |\  | | |____   ____) |
 *    |_| \_| |______| |_____/
 *
 ********************************************************/

#include "boost/program_options.hpp"
#include <Components/NesWorker.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <thread>

namespace po = boost::program_options;

using namespace NES;
using std::string;
using std::cout;
using std::endl;

const string logo = "/********************************************************\n"
                    " *     _   _   ______    _____\n"
                    " *    | \\ | | |  ____|  / ____|\n"
                    " *    |  \\| | | |__    | (___\n"
                    " *    | . ` | |  __|    \\___ \\     Worker\n"
                    " *    | |\\  | | |____   ____) |\n"
                    " *    |_| \\_| |______| |_____/\n"
                    " *\n"
                    " ********************************************************/";


// TODO handle proper configuration properly
int main(int argc, char** argv) {
    NES::setupLogging("nesWorkerStarter.log", NES::LOG_DEBUG);
    NES_INFO(logo);

    namespace po = boost::program_options;
    po::options_description desc("Nes Worker Options");
    std::string coordinatorPort = "0";
    std::string rpcPort = "3000";
    std::string dataPort = "3001";
    std::string coordinatorIp = "127.0.0.1";
    std::string localWorkerIp = "127.0.0.1";

    // set the default numberOfCpu to the number of processor
    const auto processorCount = std::thread::hardware_concurrency();
    uint16_t numberOfCpus = processorCount;

    std::string sourceType = "";
    std::string sourceConfig = "";
    size_t sourceFrequency = 0;
    size_t numberOfBuffersToProduce = 1;
    std::string physicalStreamName;
    std::string logicalStreamName;
    std::string parentId = "-1";

    desc.add_options()
        ("coordinatorPort", po::value<string>(&coordinatorPort)->default_value(coordinatorPort),
                       "Set NES rpc server port (default: 0).")
        ("rpcPort", po::value<string>(&rpcPort)->default_value(rpcPort),
         "Set NES rpc server port (default: 0).")
        ("dataPort", po::value<string>(&dataPort)->default_value(dataPort),
         "Set NES data server port (default: 0).")
        ("coordinatorIp", po::value<string>(&coordinatorIp)->default_value(coordinatorIp),
                                                                "Set NES server ip (default: 127.0.0.1).")(
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
        po::value<size_t>(&numberOfBuffersToProduce)->default_value(numberOfBuffersToProduce),
        "Set the number of buffers to produce")(
        "logicalStreamName",
        po::value<string>(&logicalStreamName)->default_value(logicalStreamName),
        "Set the logical stream name where this stream is added to")
        ("parentId", po::value<string>(&parentId)->default_value(parentId),
         "Set the parentId of this node")
        ("localWorkerIp", po::value<string>(&localWorkerIp)->default_value(localWorkerIp),
         "Set worker ip (default: 127.0.0.1)")
        ("numberOfCpus", po::value<uint16_t>(&numberOfCpus)->default_value(numberOfCpus),
         "Set the computing capacity (default: number of processor.")
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
        NES_INFO("Basic Command Line Parameter ");
        NES_INFO(desc);
        return 0;
    }

    if (argc == 1) {
        NES_INFO("Please specify at least the port you want to connect to");
        NES_INFO("Basic Command Line Parameter");
        NES_INFO(desc);
        return 0;
    }

    // TODO remote calls to cout

    size_t localPort = std::stol(rpcPort);
    size_t zmqDataPort = std::stol(dataPort);
    cout << "port=" << localPort <<  "localport=" << std::to_string(localPort)  << " pid=" << getpid() << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(
        coordinatorIp,
        coordinatorPort,
        localWorkerIp,
        localPort,
        zmqDataPort,
        numberOfCpus,
        NodeType::Sensor // TODO what is this?!
    );

    //register phy stream if nessesary
    if (sourceType != "") {
        cout << "start with dedicated source=" << sourceType <<
             endl;
        PhysicalStreamConfig conf;
        conf.sourceType = sourceType;
        conf.sourceConfig = sourceConfig;
        conf.sourceFrequency = sourceFrequency;
        conf.numberOfBuffersToProduce = numberOfBuffersToProduce;
        conf.physicalStreamName = physicalStreamName;
        conf.logicalStreamName = logicalStreamName;

        wrk->setWitRegister(conf);
    } else if (parentId != "-1") {
        NES_INFO("start with dedicated parent=" << parentId);
        wrk->setWithParent(parentId);
    }

    wrk->start(/**blocking*/ true, /**withConnect*/ true);//blocking call
    wrk->stop(/**force*/ true);
    NES_INFO("worker started");
}
