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

#include <E2E/E2ERunner.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Util/Logger/Logger.hpp>
#include <cstring>
#include <filesystem>

const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    |     | |  __|    \\___ \\     Benchmark Runner\n"
                         " *    | |\\  | | |____   ____) |\n"
                         " *    |_| \\_| |______| |_____/\n"
                         " *\n"
                         " ********************************************************/";

class BenchmarkRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                          << callStack;

        NES_FATAL_ERROR("{}", fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;
        NES_FATAL_ERROR("{}", fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};

int main(int argc, const char* argv[]) {

    std::cout << logo << std::endl;

    // Activating and installing error listener
    NES::Logger::setupLogging("main.log", NES::LogLevel::LOG_ERROR);
    auto runner = std::make_shared<BenchmarkRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    if (argc > 3 || argc == 0) {
        std::cerr << "Error: Only --configPath= and --logPath= are allowed as a command line argument!\nExiting now..."
                  << std::endl;
        return -1;
    }

    // Iterating through the arguments
    std::unordered_map<std::string, std::string> argMap;
    for (int i = 0; i < argc; ++i) {
        auto pathArg = std::string(argv[i]);
        if (pathArg.find("--configPath") != std::string::npos) {
            argMap["configPath"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        }
        if (pathArg.find("--logPath") != std::string::npos) {
            argMap["logPath"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        }
    }

    if (argMap.size() < 2) {
        std::cerr << "Error: Missing --configPath or --logPath could not been found in arguments!" << std::endl;
        return -1;
    }

    std::string configPath = argMap["configPath"];
    std::string logPath = argMap["logPath"];
    // Reading and parsing the config yaml file
    std::cout << "parsed yaml config: " << configPath << std::endl;
    if (configPath.empty() || !std::filesystem::exists(configPath)) {
        std::cerr << "No yaml file provided or the file does not exist!" << std::endl;
        return -1;
    }

    auto e2EBenchmarkConfig = NES::Benchmark::parseYamlConfig(configPath, logPath);
    NES::Benchmark::writeHeaderToCsvFile(e2EBenchmarkConfig.getConfigOverAllRuns());

    int rpcPort = 8000, restPort = 10000;
    for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
        rpcPort += 23;
        restPort += 23;

        NES::Benchmark::executeSingleRun(configPerRun, e2EBenchmarkConfig.getConfigOverAllRuns(), rpcPort, restPort);

        NES_INFO("Done with single experiment run!");
    }

    return 0;
}
