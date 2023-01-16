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
#include <API/Schema.hpp>
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
    sourceNameToDataGenerator = {{"input1", std::make_shared<DataGeneration::DefaultDataGenerator>(0, 1000)}};

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

    configOverAllRuns.startupSleepIntervalInSeconds->setValueIfDefined(yamlConfig["startupSleepIntervalInSeconds"]);
    configOverAllRuns.numMeasurementsToCollect->setValueIfDefined(yamlConfig["numberOfMeasurementsToCollect"]);
    configOverAllRuns.experimentMeasureIntervalInSeconds->setValueIfDefined(yamlConfig["experimentMeasureIntervalInSeconds"]);
    configOverAllRuns.outputFile->setValueIfDefined(yamlConfig["outputFile"]);
    configOverAllRuns.benchmarkName->setValueIfDefined(yamlConfig["benchmarkName"]);
    configOverAllRuns.query->setValueIfDefined(yamlConfig["query"]);
    configOverAllRuns.dataProviderMode->setValueIfDefined(yamlConfig["dataProviderMode"]);
    configOverAllRuns.connectionString->setValueIfDefined(yamlConfig["connectionString"]);
    configOverAllRuns.inputType->setValueIfDefined(yamlConfig["inputType"]);
    configOverAllRuns.sourceSharing->setValueIfDefined(yamlConfig["sourceSharing"]);
    configOverAllRuns.numberOfPreAllocatedBuffer->setValueIfDefined(yamlConfig["numberOfPreAllocatedBuffer"]);
    configOverAllRuns.batchSize->setValueIfDefined(yamlConfig["batchSize"]);
    configOverAllRuns.numberOfBuffersToProduce->setValueIfDefined(yamlConfig["numberOfBuffersToProduce"]);

    auto logicalSourcesNode = yamlConfig["logicalSources"];
    if (logicalSourcesNode.IsSequence()) {
        configOverAllRuns.sourceNameToDataGenerator.clear();
        for (auto entry = logicalSourcesNode.Begin(); entry != logicalSourcesNode.End(); entry++) {
            auto node = (*entry).second;
            auto sourceName = node["name"].As<std::string>();
            if (configOverAllRuns.sourceNameToDataGenerator.contains(sourceName)) {
                NES_THROW_RUNTIME_ERROR("Logical source name has to be unique. " << sourceName << " is not unique!");
            }

            auto dataGenerator = DataGeneration::DataGenerator::createGeneratorByName(node["type"].As<std::string>(), node);
            configOverAllRuns.sourceNameToDataGenerator[sourceName] = dataGenerator;
        }
    }

    return configOverAllRuns;
}

std::string E2EBenchmarkConfigOverAllRuns::getStrLogicalSrcDataGenerators() {
    std::stringstream stringStream;
    for (auto it = sourceNameToDataGenerator.begin(); it != sourceNameToDataGenerator.end(); ++it) {
        if (it != sourceNameToDataGenerator.begin()) {
            stringStream << ", ";
        }
        stringStream << it->first << ": " << it->second->getName();
    }

    return stringStream.str();
}

size_t E2EBenchmarkConfigOverAllRuns::getTotalSchemaSize() {
    size_t size = 0;
    for (auto [logicalSource, dataGenerator] : sourceNameToDataGenerator) {
        size += dataGenerator->getSchema()->getSchemaSizeInBytes();
    }

    return size;
}
}// namespace NES::Benchmark