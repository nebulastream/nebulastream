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

#include <Util/UtilityFunctions.hpp>

#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOption.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Persistence/PhysicalStreamsPersistenceFactory.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
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

    NES::setupLogging("nesCoordinatorStarter.log", NES::getDebugLevelFromString("LOG_DEBUG"));

    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceType("NoSource");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);

    std::vector<SourceConfigPtr> sourceConfigs;

    std::map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find("=")),
                                      string(argv[i]).substr(string(argv[i]).find("=") + 1, string(argv[i]).length() - 1)));
    }

    auto workerConfigPath = commandLineParams.find("--workerConfigPath");
    auto sourceConfigPath = commandLineParams.find("--sourceConfigPath");

    if (workerConfigPath != commandLineParams.end()) {
        workerConfig->overwriteConfigWithYAMLFileInput(workerConfigPath->second);
    }

    if (sourceConfigPath != commandLineParams.end()) {
        auto sourceConfigPaths = UtilityFunctions::splitWithStringDelimiter(sourceConfigPath->second, ";");
        for (auto& path : sourceConfigPaths) {
            SourceConfigPtr curr = SourceConfig::create();
            curr->overwriteConfigWithYAMLFileInput(path);
            sourceConfigs.emplace_back(curr);
        }
    }

    if (argc >= 1) {
        workerConfig->overwriteConfigWithCommandLineInput(commandLineParams);
        sourceConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }

    if (sourceConfig->getSourceType()->getValue() != "NoSource") {
        sourceConfigs.emplace_back(sourceConfig);
    }

    NES::setLogLevel(NES::getDebugLevelFromString(workerConfig->getLogLevel()->getValue()));

    NES_INFO("NESWORKERSTARTER: Start with port=" << workerConfig->getRpcPort()->getValue() << " localport="
                                                  << workerConfig->getDataPort()->getValue() << " pid=" << getpid()
                                                  << " coordinatorPort=" << workerConfig->getCoordinatorPort()->getValue());
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);

    auto configurationPersistence =
        PhysicalStreamsPersistenceFactory::createForType(workerConfig->getConfigPersistenceType()->getValue(),
                                                         workerConfig->getConfigPersistencePath()->getValue());

    auto loadedConfigs = configurationPersistence->loadConfigurations();
    if (!loadedConfigs.empty()) {
        sourceConfigs = loadedConfigs;
    }

    // register phy stream if necessary
    if (sourceConfigs.size() > 0) {
        for (auto& config : sourceConfigs) {
            NES_INFO("start with dedicated source=" << config->getSourceType()->getValue() << "\n");
            PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(config);

            auto lNames = config->getLogicalStreamName()->getValue();

            NES_INFO("NESWORKERSTARTER: Source Config type = "
                     << config->getSourceType()->getValue() << " Config = " << config->getSourceConfig()->getValue()
                     << " physicalStreamName = " << config->getPhysicalStreamName()->getValue() << " logicalStreamName = "
                     << "(" << UtilityFunctions::combineStringsWithDelimiter(lNames, ",") + ")");

            wrk->setWithRegister(conf);
        }
    } else if (workerConfig->getParentId()->getValue() != "-1") {
        NES_INFO("start with dedicated parent=" << workerConfig->getParentId()->getValue());
        wrk->setWithParent(workerConfig->getParentId()->getValue());
    }

    wrk->start(/**blocking*/ true, /**withConnect*/ true);//blocking call
    wrk->stop(/**force*/ true);
    NES_INFO("worker started");
}
