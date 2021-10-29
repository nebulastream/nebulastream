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

#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOption.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Util/Logger.hpp>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
using namespace NES;
using namespace Configurations;
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

int main(int argc, char** argv) {
    std::cout << logo << std::endl;

    NES::setupLogging("nesCoordinatorStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));

    WorkerConfigPtr workerConfig = WorkerConfig::create();

    std::map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    auto workerConfigPath = commandLineParams.find("--workerConfigPath");

    if (workerConfigPath != commandLineParams.end()) {
        workerConfig->overwriteConfigWithYAMLFileInput(workerConfigPath->second);
    }

    if (argc >= 1) {
        workerConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }

    SourceConfigPtr sourceConfig = SourceConfigFactory::createSourceConfig(commandLineParams, argc);

    NES::setLogLevel(NES::getDebugLevelFromString(workerConfig->getLogLevel()->getValue()));

    NES_INFO("NESWORKERSTARTER: Start with " << workerConfig->toString());
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);

    //register phy stream if necessary
    if (sourceConfig->getSourceType()->getValue() != "NoSource") {
        NES_INFO("start with dedicated source=" << sourceConfig->getSourceType()->getValue() << "\n");
        PhysicalStreamConfigPtr physicalStreamConfig = PhysicalStreamConfig::create(sourceConfig);

        NES_INFO("NESWORKERSTARTER: Source Config: " << sourceConfig->toString());

        wrk->setWithRegister(physicalStreamConfig);
    } else if (workerConfig->getParentId()->getValue() != "-1") {
        NES_INFO("start with dedicated parent=" << workerConfig->getParentId()->getValue());
        wrk->setWithParent(workerConfig->getParentId()->getValue());
    }


    try {
        wrk->start(/**blocking*/ true, /**withConnect*/ true);//blocking call
        wrk->stop(/**force*/ true);
    }
    catch(std::exception& exp) {
        NES_ERROR("Problem with Worker:  << " << exp.what());
    }
    catch(...) {
        throw;
    }
    NES_INFO("worker started");
}
