/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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
#include <iostream>
#include <thread>

namespace po = boost::program_options;

using namespace NES;
using std::cout;
using std::endl;
using std::string;

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
    std::cout << logo << std::endl;

    namespace po = boost::program_options;
    po::options_description desc("Nes Worker Options");
    std::string coordinatorPort = "0";
    std::string rpcPort = "3000";
    std::string dataPort = "3001";
    std::string coordinatorIp = "127.0.0.1";
    std::string localWorkerIp = "127.0.0.1";
    std::string logLevel = "LOG_DEBUG";

    // set the default numberOfSlots to the number of processor
    const auto processorCount = std::thread::hardware_concurrency();
    uint16_t numberOfSlots = processorCount;

    std::string sourceType = "";
    std::string sourceConfig = "";
    size_t sourceFrequency = 0;
    size_t numberOfBuffersToProduce = 1;
    std::string physicalStreamName;
    std::string logicalStreamName;
    std::string parentId = "-1";
    std::string endlessRepeat = "";
    std::string skipHeader = "false";
    size_t numberOfTuplesToProducePerBuffer = 0;

    desc.add_options()("coordinatorPort", po::value<string>(&coordinatorPort)->default_value(coordinatorPort),
                       "Set NES rpc server port (default: 0).")("rpcPort", po::value<string>(&rpcPort)->default_value(rpcPort),
                                                                "Set NES rpc server port (default: 0).")(
        "dataPort", po::value<string>(&dataPort)->default_value(dataPort), "Set NES data server port (default: 0).")(
        "coordinatorIp", po::value<string>(&coordinatorIp)->default_value(coordinatorIp),
        "Set NES server ip (default: 127.0.0.1).")("sourceType", po::value<string>(&sourceType)->default_value(sourceType),
                                                   "Set the type of the Source either CSVSource or DefaultSource")(
        "sourceConfig", po::value<string>(&sourceConfig)->default_value(sourceConfig),
        "Set the config for the source e.g. the file name")(
        "sourceFrequency", po::value<size_t>(&sourceFrequency)->default_value(sourceFrequency), "Set the sampling frequency")(
        "endlessRepeat", po::value<string>(&endlessRepeat)->default_value("off"), "Looping endless over the file")(
        "skipHeader", po::value<string>(&skipHeader)->default_value("false"), "Skip first line of the file (default=false)")(
        "physicalStreamName", po::value<string>(&physicalStreamName)->default_value(physicalStreamName),
        "Set the physical name of the stream")(
        "numberOfBuffersToProduce", po::value<size_t>(&numberOfBuffersToProduce)->default_value(numberOfBuffersToProduce),
        "Set the number of buffers to produce")("numberOfTuplesToProducePerBuffer",
                                                po::value<size_t>(&numberOfTuplesToProducePerBuffer)->default_value(0),
                                                "Set the number of buffers to produce")(
        "logicalStreamName", po::value<string>(&logicalStreamName)->default_value(logicalStreamName),
        "Set the logical stream name where this stream is added to")(
        "parentId", po::value<string>(&parentId)->default_value(parentId), "Set the parentId of this node")(
        "localWorkerIp", po::value<string>(&localWorkerIp)->default_value(localWorkerIp),
        "Set worker ip (default: 127.0.0.1)")("numberOfSlots", po::value<uint16_t>(&numberOfSlots)->default_value(numberOfSlots),
                                              "Set the computing capacity (default: number of processor.")(
        "logLevel", po::value<std::string>(&logLevel)->default_value(logLevel),
        "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)")("help", "Display help message");

    /* Parse parameters. */
    po::variables_map vm;
    try {
        po::store(po::command_line_parser(argc, argv).options(desc).run(), vm);
        po::notify(vm);
    } catch (const std::exception& e) {
        std::cerr << "Failure while parsing connection parameters!" << std::endl;
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    NES::setupLogging("nesCoordinatorStarter.log", NES::getStringAsDebugLevel(logLevel));

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
    cout << "port=" << localPort << "localport=" << std::to_string(localPort) << " pid=" << getpid() << endl;
    NesWorkerPtr wrk =
        std::make_shared<NesWorker>(coordinatorIp, coordinatorPort, localWorkerIp, localPort, zmqDataPort, numberOfSlots,
                                    NodeType::Sensor// TODO what is this?!
        );

    //register phy stream if nessesary
    if (sourceType != "") {
        bool endless = endlessRepeat == "on";
        bool skip = skipHeader == "true";
        cout << "start with dedicated source=" << sourceType << " endlessRepeat=" << endlessRepeat << " end=" << endless << endl;
        PhysicalStreamConfigPtr conf =
            PhysicalStreamConfig::create(sourceType, sourceConfig, sourceFrequency, numberOfTuplesToProducePerBuffer,
                                         numberOfBuffersToProduce, physicalStreamName, logicalStreamName, endless, skip);

        wrk->setWithRegister(conf);
    } else if (parentId != "-1") {
        NES_INFO("start with dedicated parent=" << parentId);
        wrk->setWithParent(parentId);
    }

    wrk->start(/**blocking*/ true, /**withConnect*/ true);//blocking call
    wrk->stop(/**force*/ true);
    NES_INFO("worker started");
}
