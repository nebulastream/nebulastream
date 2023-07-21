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

#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkASPUtil.hpp>
#include <Experimental/Benchmarking/MicroBenchmarkRun.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Common.hpp>
#include <iostream>

const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    |     | |  __|    \\___ \\     Micro-Benchmark ASP \n"
                         " *    | |\\  | | |____   ____) |\n"
                         " *    |_| \\_| |______| |_____/\n"
                         " *\n"
                         " ********************************************************/";

class MicroBenchmarkRunner : public NES::Exceptions::ErrorListener {
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
    using namespace NES;

    std::cout << logo << std::endl;

    // Activating and installing error listener
    Logger::setupLogging("main.log", NES::LogLevel::LOG_DEBUG);
    auto runner = std::make_shared<MicroBenchmarkRunner>();
    Exceptions::installGlobalErrorListener(runner);

    if (argc > 3 || argc == 0) {
        std::cerr << "Too many arguments error: Only --configPath= and --dataPath= are allowed as a command line argument!\nExiting now..."
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
        if (pathArg.find("--dataPath") != std::string::npos) {
            argMap["dataPath"] = pathArg.substr(pathArg.find("=") + 1, pathArg.length() - 1);
        }
    }

    auto yamlFileName = argMap["configPath"];
    auto absoluteDataPath = std::filesystem::absolute(std::filesystem::path(argMap["dataPath"]));

    NES_INFO("Parsing all micro-benchmarks...");
    auto csvFileName = ASP::Util::parseCsvFileFromYaml(yamlFileName);
    auto allMicroBenchmarks = ASP::Benchmarking::MicroBenchmarkRun::parseMicroBenchmarksFromYamlFile(yamlFileName,
                                                                                                     absoluteDataPath);

    NES_INFO("Running all micro-benchmarks...");
    for (auto& microBenchmark : allMicroBenchmarks) {
        NES_DEBUG("Running current micro-benchmark: {}", microBenchmark.toString());
        microBenchmark.run();
    }

    NES_INFO("Writing the header to the csv file: {}", csvFileName);
    NES::Util::writeHeaderToCsvFile(csvFileName, allMicroBenchmarks[0].getHeaderAsCsv());

    NES_INFO("Writing all micro-benchmarks results to csv file: {}", csvFileName);
    for (auto& microBenchmark : allMicroBenchmarks) {
        NES::Util::writeRowToCsvFile(csvFileName, microBenchmark.getRowsAsCsv());
    }

    NES_INFO("Done running micro-benchmarks!");
}