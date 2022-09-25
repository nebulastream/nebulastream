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

#include <E2EBenchmarkConfigOverAllRuns.hpp>

NES::Benchmark::E2EBenchmarkConfigOverAllRuns::E2EBenchmarkConfigOverAllRuns()  {
    using namespace Configurations;
    startupSleepIntervalInSeconds = ConfigurationOption<uint32_t>::create("startupSleepIntervalInSeconds", 5,
                                                                          "Time the benchmark waits after query submission");
    numMeasurementsToCollect = ConfigurationOption<uint32_t>::create("numMeasurementsToCollect", 10,
                                                                     "Number of measurements taken before terminating a benchmark!");
    experimentMeasureIntervalInSeconds = ConfigurationOption<uint32_t>::create("experimentMeasureIntervalInSeconds", 1,
                                                                               "Measuring duration of one sample");

    outputFile = ConfigurationOption<std::string>::create("outputFile", "e2eBenchmarkRunner", "Filename of the output");
    benchmarkName = ConfigurationOption<std::string>::create("benchmarkName", "E2ERunner", "Name of the benchmark");
    inputType = ConfigurationOption<std::string>::create("inputType", "Auto", "How to read the input data");
    query = ConfigurationOption<std::string>::create("query", "", "Query to be run");
}
std::string NES::Benchmark::E2EBenchmarkConfigOverAllRuns::toString()  {
    std::stringstream oss;
    oss << "- startupSleepIntervalInSeconds: " << startupSleepIntervalInSeconds->getValueAsString() << std::endl
        << "- numMeasurementsToCollect: " << numMeasurementsToCollect->getValueAsString() << std::endl
        << "- experimentMeasureIntervalInSeconds: " << experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
        << "- outputFile: " << outputFile->getValue() << std::endl
        << "- benchmarkName: " << benchmarkName->getValue() << std::endl
        << "- inputType: " << inputType->getValue() << std::endl
        << "- query: " << query->getValue() << std::endl;


    return oss.str();
}
