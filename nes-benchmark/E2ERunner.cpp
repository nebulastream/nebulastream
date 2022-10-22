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


#include <E2E/E2EBenchmarkConfig.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Util/Logger/Logger.hpp>

#include <E2E/E2ESingleRun.hpp>
#include <cstring>
#include <filesystem>
#include <fstream>

namespace NES::Exceptions {
    extern void installGlobalErrorListener(std::shared_ptr<ErrorListener> const&);
}
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
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack " << callStack;

        NES_FATAL_ERROR(fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR(fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};

int main(int argc, const char* argv[]) {


    std::cout << logo << std::endl;

    // Activating and installing error listener
    auto runner = std::make_shared<BenchmarkRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    if (argc > 2 || argc == 0) {
        std::cerr << "Error: Only --configPath= is allowed as a command line argument!\nExiting now..." << std::endl;
        return -1;
    }

    auto configPathArg = std::string(argv[1]);
    if (configPathArg.find("--configPath") == std::string::npos) {
        std::cerr << "Error: --configPath could not been found in " << configPathArg << "!" << std::endl;
        return -1;
    }

    // Reading and parsing the config yaml file
    auto configPath = configPathArg.substr(configPathArg.find("=") + 1, configPathArg.length() - 1);
    std::cout << "parsed yaml config: " << configPath << std::endl;
    if (configPath.empty() || !std::filesystem::exists(configPath)) {
        std::cerr << "No yaml file provided or the file does not exist!" << std::endl;
        return -1;
    }


    NES::Logger::setupLogging("e2eRunnerStartup.log", NES::Benchmark::E2EBenchmarkConfig::getLogLevel(configPath));
    NES::Benchmark::E2EBenchmarkConfig e2EBenchmarkConfig;

    try {
        e2EBenchmarkConfig = NES::Benchmark::E2EBenchmarkConfig::createBenchmarks(configPath);
        NES_INFO("E2ERunner: Created the following experiments: " << e2EBenchmarkConfig.toString());
    } catch (std::exception& e) {
        NES_ERROR("E2ERunner: Error while creating benchmarks!");
        return -1;
    }

    // Writing the csv header to the output file
    std::ofstream ofs;
    ofs.open(e2EBenchmarkConfig.getConfigOverAllRuns().outputFile->getValue(), std::ofstream::out | std::ofstream::app);
    ofs << "BM_NAME,NES_VERSION,SchemaSizeInB"
        << ",Time,ProcessedTasks,ProcessedBuffers,ProcessedTuples,LatencySum,QueueSizeSum"
        << ",ThroughputInTuplesPerSec,ThroughputInTasksPerSec,ThroughputInBuffersPerSec,ThroughputInMiBPerSec"
        << ",WorkerThreads,SourceCnt,BufferSizeInB,InputType,DataProviderMode,Query"
        << "\n";
    ofs.close();

//    int portOffset = 0;
//    auto configOverAllRuns = e2EBenchmarkConfig.getConfigOverAllRuns();
//    for (auto& configPerRun : e2EBenchmarkConfig.getAllConfigPerRuns()) {
//        portOffset += 23;
//        NES::Benchmark::E2ESingleRun singleRun(configPerRun,
//                                               e2EBenchmarkConfig.getConfigOverAllRuns(),
//                                               portOffset);
//
//        singleRun.run();
//
//        NES_INFO("Done with single experiment run!");
//    }

}