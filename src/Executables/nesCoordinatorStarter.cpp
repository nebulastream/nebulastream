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
    std::string restIp = "127.0.0.1";
    std::string rpcIp = "127.0.0.1";
    std::string logLevel = "LOG_DEBUG";

    // set the default numberOfSlots to the number of processor
    const auto processorCount = std::thread::hardware_concurrency();
    uint16_t numberOfSlots = processorCount;
    bool enableQueryMerging = false;

    po::options_description serverOptions("Nes Coordinator Server Options");

    serverOptions.add_options()("restIp", po::value<std::string>(&restIp)->default_value(restIp),
                                    "Set NES ip of the REST server (default: 127.0.0.1).")(
                                ("restPort", po::value<uint16_t>(),
                                    "Set NES REST server port (default: 8081).")
                                ("coordinatorPort", po::value<uint16_t>(&rpcPort)->default_value(rpcPort),
                                    "Set NES rpc server port (default: 4000).")
                                ("numberOfSlots", po::value<uint16_t>(&numberOfSlots)->default_value(numberOfSlots),
                                    "Set the computing capacity (default: number of processor).")
                                ("enableQueryMerging", po::value<bool>(&enableQueryMerging)->default_value(enableQueryMerging),
                                    "Enable Query Merging Feature (default: false).")
                                ("logLevel", po::value<std::string>(&logLevel)->default_value(logLevel),
                                    "The log level (LOG_NONE, LOG_WARNING, LOG_DEBUG, LOG_INFO, LOG_TRACE)")
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
    NES::setupLogging("nesCoordinatorStarter.log", NES::getStringAsDebugLevel(logLevel));

    if (vm.count("help")) {
        NES_INFO("Basic Command Line Parameter ");
        NES_INFO(serverOptions);
        return 0;
    }

    NES_INFO("creating coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(restIp, restPort, rpcIp, rpcPort, numberOfSlots, enableQueryMerging);

    NES_INFO("start coordinator with RestIp=" << restIp << " restPort=" << restPort << " rpcIp=" << rpcIp << " with rpc port "
                                              << rpcPort << " numberOfSlots=" << numberOfSlots);
    crd->startCoordinator(/**blocking**/ true);//blocking call
    crd->stopCoordinator(true);
    NES_INFO("coordinator started");
}
