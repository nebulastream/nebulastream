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

#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <Util/yaml/Yaml.hpp>
#include <DataGeneration/DefaultDataGenerator.hpp>

namespace NES::Benchmark {
E2EBenchmarkConfigOverAllRuns::E2EBenchmarkConfigOverAllRuns() {
    using namespace Configurations;
    startupSleepIntervalInSeconds = ConfigurationOption<uint32_t>::create("startupSleepIntervalInSeconds",
                                                                          5,
                                                                          "Time the benchmark waits after query submission");
    numMeasurementsToCollect =
        ConfigurationOption<uint32_t>::create("numMeasurementsToCollect",
                                              10,
                                              "Number of measurements taken before terminating a benchmark!");
    experimentMeasureIntervalInSeconds =
        ConfigurationOption<uint32_t>::create("experimentMeasureIntervalInSeconds", 1, "Measuring duration of one sample");

    numberOfPreAllocatedBuffer = ConfigurationOption<uint32_t>::create("numberOfPreAllocatedBuffer", 1, "Pre-allocated buffer");
    outputFile = ConfigurationOption<std::string>::create("outputFile", "e2eBenchmarkRunner", "Filename of the output");
    benchmarkName = ConfigurationOption<std::string>::create("benchmarkName", "E2ERunner", "Name of the benchmark");
    query = ConfigurationOption<std::string>::create("query", "", "Query to be run");
    inputType = ConfigurationOption<std::string>::create("inputType", "Auto", "If sources are shared");
    sourceSharing = ConfigurationOption<std::string>::create("sourceSharing", "off", "How to read the input data");
    dataProviderMode =
        ConfigurationOption<std::string>::create("dataProviderMode", "ZeroCopy", "DataProviderMode either ZeroCopy or MemCopy");
    connectionString = ConfigurationOption<std::string>::create("connectionString", "", "Optional string to connect to source");
    numberOfBuffersToProduce = ConfigurationOption<uint32_t>::create("numBuffersToProduce", 5000000, "No. buffers to produce");
    batchSize = ConfigurationOption<uint32_t>::create("batchSize", 1, "Number of messages pulled in one chunk");

    srcNameToDataGenerator = {{"input1", std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000)}};

}
std::string E2EBenchmarkConfigOverAllRuns::toString() {
    std::stringstream oss;
    oss << "- startupSleepIntervalInSeconds: " << startupSleepIntervalInSeconds->getValueAsString() << std::endl
        << "- numMeasurementsToCollect: " << numMeasurementsToCollect->getValueAsString() << std::endl
        << "- experimentMeasureIntervalInSeconds: " << experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
        << "- outputFile: " << outputFile->getValue() << std::endl
        << "- benchmarkName: " << benchmarkName->getValue() << std::endl
        << "- inputType: " << inputType->getValue() << std::endl
        << "- sourceSharing: " << sourceSharing->getValue() << std::endl
        << "- query: " << query->getValue() << std::endl
        << "- numberOfPreAllocatedBuffer: " << numberOfPreAllocatedBuffer->getValueAsString() << std::endl
        << "- numberOfBuffersToProduce: " << numberOfBuffersToProduce->getValueAsString() << std::endl
        << "- batchSize: " << batchSize->getValueAsString() << std::endl
        << "- dataProviderMode: " << dataProviderMode->getValue() << std::endl
        << "- connectionString: " << connectionString->getValue() << std::endl
        << "- logicalSources: " << getStrLogicalSrcDataGenerators() << std::endl;

    return oss.str();
}

E2EBenchmarkConfigOverAllRuns E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(Yaml::Node yamlConfig) {
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;
    configOverAllRuns.startupSleepIntervalInSeconds->setValue(yamlConfig["startupSleepIntervalInSeconds"].As<uint32_t>());
    configOverAllRuns.numMeasurementsToCollect->setValue(yamlConfig["numberOfMeasurementsToCollect"].As<uint32_t>());
    configOverAllRuns.experimentMeasureIntervalInSeconds->setValue(
        yamlConfig["experimentMeasureIntervalInSeconds"].As<uint32_t>());
    configOverAllRuns.outputFile->setValue(yamlConfig["outputFile"].As<std::string>());
    configOverAllRuns.benchmarkName->setValue(yamlConfig["benchmarkName"].As<std::string>());
    configOverAllRuns.query->setValue(yamlConfig["query"].As<std::string>());
    configOverAllRuns.dataProviderMode->setValue(yamlConfig["dataProviderMode"].As<std::string>());
    configOverAllRuns.connectionString->setValue(yamlConfig["connectionString"].As<std::string>());
    configOverAllRuns.inputType->setValue(yamlConfig["inputType"].As<std::string>());
    configOverAllRuns.sourceSharing->setValue(yamlConfig["sourceSharing"].As<std::string>());
    configOverAllRuns.numberOfPreAllocatedBuffer->setValue(yamlConfig["numberOfPreAllocatedBuffer"].As<uint32_t>());
    configOverAllRuns.batchSize->setValue(yamlConfig["batchSize"].As<uint32_t>());
    configOverAllRuns.numberOfBuffersToProduce->setValue(yamlConfig["numberOfBuffersToProduce"].As<uint32_t>());

    auto logicalSourcesNode = yamlConfig["logicalSources"];
    if (logicalSourcesNode.IsMap()) {
        for (auto entry = logicalSourcesNode.Begin(); entry != logicalSourcesNode.End(); entry++) {
            auto sourceName = (*entry).first;
            auto node = (*entry).second;
            if (configOverAllRuns.srcNameToDataGenerator.contains(sourceName)) {
                NES_THROW_RUNTIME_ERROR("Logical source name has to be unique. " << sourceName << " is not unique!");
            }

            auto dataGenerator = DataGeneration::DataGenerator::createGeneratorByName(sourceName, node);
            configOverAllRuns.srcNameToDataGenerator[sourceName] = dataGenerator;
        }
    }

    return configOverAllRuns;
}


std::string E2EBenchmarkConfigOverAllRuns::getStrLogicalSrcDataGenerators() {
    std::stringstream stringStream;
    for (auto it = srcNameToDataGenerator.begin(); it != srcNameToDataGenerator.end(); ++it) {
        if (it != srcNameToDataGenerator.begin()) {
            stringStream << ", ";
        }
        stringStream << it->first << ": " << it->second;
    }

    return stringStream.str();
}

}// namespace NES::Benchmark