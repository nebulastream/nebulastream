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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Synopses/AbstractSynopses.hpp>
#include <Synopses/Samples/SampleRandomWithReplacement.hpp>
#include <Util/Logger/Logger.hpp>
#include <Benchmarking/Util.hpp>
#include <iostream>

const std::string logo = "/********************************************************\n"
                         " *     _   _   ______    _____\n"
                         " *    | \\ | | |  ____|  / ____|\n"
                         " *    |  \\| | | |__    | (___\n"
                         " *    |     | |  __|    \\___ \\     Microbenchmark ASP \n"
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
    using namespace NES;

    std::cout << logo << std::endl;

    // Activating and installing error listener
    Logger::setupLogging("main.log", NES::LogLevel::LOG_INFO);
    auto runner = std::make_shared<MicroBenchmarkRunner>();
    Exceptions::installGlobalErrorListener(runner);

    if (argc != 0) {
        std::cerr << "Error: Only --configPath= is allowed as a command line argument!\nExiting now..." << std::endl;
        return -1;
    }
    auto yamlFileName = std::string(argv[1]);

    // Parsing the yaml file
    auto allSynopsisArguments = ASP::SynopsesArguments::parseArgumentsFromYamlFile(yamlFileName);
    auto csvFileName = ASP::Util::parseCsvFileFromYaml(yamlFileName);

    // Iterating over all synopsis argument and running the micro benchmarks
    for (auto& synopsisArguments : allSynopsisArguments) {
        hier weiter machen
    }




    /**
     * 1. Parse yaml file into SynopsisArguments
     * 2. Create synopsis from SynopsisArguments
     * 3. Create input data (maybe read it from a csv file)
     * 4. Run addToSynopsis() and getApproximate()
     * 5. Calculate throughput and write this into a csv file together with the rest of the params
     *
     * Once this is done, create a key version for synopsis
     */

    // Parsing the yaml file from the command line





    size_t sampleSize = 1000;
    auto fieldName = "f1";

    PhysicalTypePtr dataType = DefaultPhysicalTypeFactory().getPhysicalType(DataTypeFactory::createInt8());
    auto averageAggregationFunction = std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(dataType, dataType);

    auto sampleArguments = ASP::SRSWR(sampleSize, fieldName, averageAggregationFunction);
    auto synopsis = ASP::AbstractSynopses::create(sampleArguments);

}