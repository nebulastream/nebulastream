/*
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

#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Exceptions/SignalHandling.hpp>
#include <Util/Logger/Logger.hpp>
#include <Version/version.hpp>
#include <iostream>

using namespace NES;
using namespace Configurations;

const std::string logo =
    "\n"
    "███╗░░██╗███████╗██████╗░██╗░░░██╗██╗░░░░░░█████╗░░██████╗████████╗██████╗░███████╗░█████╗░███╗░░░███╗\n"
    "████╗░██║██╔════╝██╔══██╗██║░░░██║██║░░░░░██╔══██╗██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗░████║\n"
    "██╔██╗██║█████╗░░██████╦╝██║░░░██║██║░░░░░███████║╚█████╗░░░░██║░░░██████╔╝█████╗░░███████║██╔████╔██║\n"
    "██║╚████║██╔══╝░░██╔══██╗██║░░░██║██║░░░░░██╔══██║░╚═══██╗░░░██║░░░██╔══██╗██╔══╝░░██╔══██║██║╚██╔╝██║\n"
    "██║░╚███║███████╗██████╦╝╚██████╔╝███████╗██║░░██║██████╔╝░░░██║░░░██║░░██║███████╗██║░░██║██║░╚═╝░██║\n"
    "╚═╝░░╚══╝╚══════╝╚═════╝░░╚═════╝░╚══════╝╚═╝░░╚═╝╚═════╝░░░░╚═╝░░░╚═╝░░╚═╝╚══════╝╚═╝░░╚═╝╚═╝░░░░░╚═╝";

const std::string worker = "\n"
                           "▒█░░▒█ █▀▀█ █▀▀█ █░█ █▀▀ █▀▀█ \n"
                           "▒█▒█▒█ █░░█ █▄▄▀ █▀▄ █▀▀ █▄▄▀ \n"
                           "▒█▄▀▄█ ▀▀▀▀ ▀░▀▀ ▀░▀ ▀▀▀ ▀░▀▀";

extern void Exceptions::installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);

int main(int argc, char** argv) {
    try {
        std::cout << logo << std::endl;
        std::cout << worker << "v" << NES_VERSION << std::endl;
        NES::Logger::setupLogging("nesWorkerStarter.log", NES::LogLevel::LOG_DEBUG);
        WorkerConfigurationPtr workerConfiguration = WorkerConfiguration::create();

        std::map<std::string, std::string> commandLineParams;
        for (int i = 1; i < argc; ++i) {
            commandLineParams.insert(std::pair<std::string, std::string>(
                std::string(argv[i]).substr(0, std::string(argv[i]).find('=')),
                std::string(argv[i]).substr(std::string(argv[i]).find('=') + 1, std::string(argv[i]).length() - 1)));
        }

        auto workerConfigPath = commandLineParams.find("--configPath");
        //if workerConfigPath to a yaml file is provided, system will use physicalSources in yaml file
        if (workerConfigPath != commandLineParams.end()) {
            workerConfiguration->configPath = workerConfigPath->second;
            workerConfiguration->overwriteConfigWithYAMLFileInput(workerConfigPath->second);
        }

        //if command line params are provided that do not contain a path to a yaml file for worker config,
        //command line param physicalSources are used to overwrite default physicalSources
        if (argc >= 1 && !commandLineParams.contains("--configPath")) {
            workerConfiguration->overwriteConfigWithCommandLineInput(commandLineParams);
        }

        Logger::getInstance()->changeLogLevel(workerConfiguration->logLevel.getValue());

        NES_INFO("NesWorkerStarter: Start with {}", workerConfiguration->toString());
        auto nesWorker = std::make_shared<NesWorker>(std::move(workerConfiguration));
        Exceptions::installGlobalErrorListener(nesWorker);

        NES_INFO("Starting worker");
        nesWorker->start(/**blocking*/ true, /**withConnect*/ true);//This is a blocking call
        NES_INFO("Stopping worker");
        nesWorker->stop(/**force*/ true);
    } catch (std::exception& exp) {
        NES_ERROR("Problem with worker: {}", exp.what());
        return 1;
    } catch (...) {
        NES_ERROR("Unknown exception was thrown");
        try {
            std::rethrow_exception(std::current_exception());
        } catch (std::exception& ex) {
            NES_ERROR("{}", ex.what());
        }
        return 1;
    }
}
