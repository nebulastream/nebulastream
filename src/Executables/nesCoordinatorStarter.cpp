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
#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <GRPC/CoordinatorRPCServer.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <map>
#include <sys/stat.h>
#include <vector>

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

    NES::setupLogging("nesCoordinatorStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();

    map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                 string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    auto configPath = commandLineParams.find("--configPath");

    if (configPath != commandLineParams.end()) {
        coordinatorConfig->overwriteConfigWithYAMLFileInput(configPath->second);
    }
    if (argc > 1) {
        coordinatorConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }
    NES::setLogLevel(NES::getDebugLevelFromString(coordinatorConfig->getLogLevel()->getValue()));

    NES_INFO("start coordinator with RestIp=" << coordinatorConfig->getRestIp()->getValue()
                                              << " restPort=" << coordinatorConfig->getRestPort()->getValue()
                                              << " coordinatorIp=" << coordinatorConfig->getCoordinatorIp()->getValue()
                                              << " with rpc port " << coordinatorConfig->getRpcPort()->getValue()
                                              << " numberOfThreads=" << coordinatorConfig->getNumWorkerThreads()->getValue()
                                              << " numberOfSlots=" << coordinatorConfig->getNumberOfSlots()->getValue());

    NES_INFO("creating coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);

    crd->startCoordinator(/**blocking**/ true);//blocking call
    crd->stopCoordinator(true);
    NES_INFO("coordinator started");
}
