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
#include <Configs/ConfigOption.hpp>
#include <Configs/ConfigOptions/WorkerConfig.hpp>
#include <Configs/ConfigOptions/SourceConfig.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <sys/stat.h>
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

    NES::setupLogging("nesCoordinatorStarter.log", NES::getStringAsDebugLevel("LOG_DEBUG"));

    WorkerConfig* workerConfig = new WorkerConfig();
    SourceConfig* sourceConfig = new SourceConfig();

    if (argc == 1) {
        NES_INFO("NESWORKERSTARTER: Please specify at least the port you want to connect to");
        NES_INFO("NESWORKERSTARTER: Basic Command Line Parameter");
        return 0;
    }

    map<string, string> commandLineParams;

    for (int i = 1; i < argc; ++i) {
        commandLineParams.insert(
            pair<string,string>(
                string(argv[i]).substr(0, string(argv[i]).find("=")),
                string(argv[i]).substr(string(argv[i]).find("=")+1, string(argv[i]).length()-1)));
    }

    auto workerConfigPath = commandLineParams.find("--workerConfigPath");
    auto sourceConfigPath = commandLineParams.find("--sourceConfigPath");

    if (workerConfigPath != commandLineParams.end()) {
//        workerConfig->overwriteConfigWithYAMLFileInput(workerConfigPath->second, config);
    }
    //config.Clear();
    if (sourceConfigPath != commandLineParams.end()) {
        //sourceConfig->overwriteConfigWithYAMLFileInput(sourceConfigPath->second, config);
    }
    if (argc >= 1) {
        workerConfig->overwriteConfigWithCommandLineInput(commandLineParams);
        sourceConfig->overwriteConfigWithCommandLineInput(commandLineParams);
    }
    NES::setLogLevel(NES::getStringAsDebugLevel(workerConfig->getLogLevel().getValue()));

    NES_INFO("NESWORKERSTARTER: Start with port=" << workerConfig->getRpcPort().getValue() << " localport=" << workerConfig->getDataPort().getValue() << " pid=" << getpid()
                                                  << " coordinatorPort=" << workerConfig->getCoordinatorPort().getValue());
    NesWorkerPtr wrk =
        std::make_shared<NesWorker>(workerConfig->getCoordinatorIp().getValue(), workerConfig->getCoordinatorPort().getValue(),
                                    workerConfig->getLocalWorkerIp().getValue(), workerConfig->getRpcPort().getValue(),
                                    workerConfig->getDataPort().getValue(), workerConfig->getNumberOfSlots().getValue(),
                                    NodeType::Sensor,// TODO what is this?!
                                    workerConfig->getNumWorkerThreads().getValue());

    //register phy stream if necessary
    if (sourceConfig->getSourceType().getValue() != "NoSource") {
        NES_INFO("start with dedicated source=" << sourceConfig->getSourceType().getValue() << "\n");
        PhysicalStreamConfigPtr conf =
            PhysicalStreamConfig::create(sourceConfig->getSourceType().getValue(), sourceConfig->getSourceConfig().getValue(),
                                         sourceConfig->getSourceFrequency().getValue(), sourceConfig->getNumberOfTuplesToProducePerBuffer().getValue(),
                                         sourceConfig->getNumberOfBuffersToProduce().getValue(), sourceConfig->getPhysicalStreamName().getValue(),
                                         sourceConfig->getLogicalStreamName().getValue(), sourceConfig->getSkipHeader().getValue());

        wrk->setWithRegister(conf);
    } else if (workerConfig->getParentId().getValue() != "-1") {
        NES_INFO("start with dedicated parent=" << workerConfig->getParentId().getValue());
        wrk->setWithParent(workerConfig->getParentId().getValue());
    }

    wrk->start(/**blocking*/ true, /**withConnect*/ true);//blocking call
    wrk->stop(/**force*/ true);
    NES_INFO("worker started");
}
