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
#include <DataGeneration/DefaultDataGenerator.hpp>
#include <E2E/Configurations/E2EBenchmarkConfigOverAllRuns.hpp>
#include <Util/yaml/Yaml.hpp>

constexpr auto defaultCustomDelayInSeconds = 0;

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
    queries.emplace_back("", defaultCustomDelayInSeconds);
    inputType = ConfigurationOption<std::string>::create("inputType", "Auto", "If sources are shared");
    sourceSharing = ConfigurationOption<std::string>::create("sourceSharing", "off", "How to read the input data");
    dataProviderMode =
        ConfigurationOption<std::string>::create("dataProviderMode", "ZeroCopy", "DataProviderMode either ZeroCopy or MemCopy");
    connectionString = ConfigurationOption<std::string>::create("connectionString", "", "Optional string to connect to source");
    numberOfBuffersToProduce = ConfigurationOption<uint32_t>::create("numBuffersToProduce", 5000000, "No. buffers to produce");
    batchSize = ConfigurationOption<uint32_t>::create("batchSize", 1, "Number of messages pulled in one chunk");
    sourceNameToDataGenerator["input1"] = std::make_unique<DataGeneration::DefaultDataGenerator>(0, 1000);
    ingestionRateInBuffers =
        ConfigurationOption<uint32_t>::create("ingestionRateInBuffers", 50000, "Number of buffers ingested per time interval");
    ingestionRateCount =
        ConfigurationOption<uint32_t>::create("ingestionRateCount", 10000, "Number of potentially different ingestion rates");
    numberOfPeriods =
        ConfigurationOption<uint32_t>::create("numberOfPeriods", 1, "Number of periods for sine and cosine distribution");
    ingestionRateDistribution =
        ConfigurationOption<std::string>::create("ingestionRateDistribution", "Uniform", "Type of ingestion rate distribution");
    customValues = ConfigurationOption<std::string>::create("customValues", "50000", "A vector of custom ingestion rates");
    dataProvider = ConfigurationOption<std::string>::create("dataProvider", "Internal", "Type of data provider");
    joinStrategy = ConfigurationOption<std::string>::create("joinStrategy", "HASH_JOIN_LOCAL", "Applied Join Algorithm");
}

std::string E2EBenchmarkConfigOverAllRuns::toString() const {
    std::stringstream oss;
    oss << "- startupSleepIntervalInSeconds: " << startupSleepIntervalInSeconds->getValueAsString() << std::endl
        << "- numMeasurementsToCollect: " << numMeasurementsToCollect->getValueAsString() << std::endl
        << "- experimentMeasureIntervalInSeconds: " << experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
        << "- outputFile: " << outputFile->getValue() << std::endl
        << "- benchmarkName: " << benchmarkName->getValue() << std::endl
        << "- inputType: " << inputType->getValue() << std::endl
        << "- sourceSharing: " << sourceSharing->getValue() << std::endl
        << "- query: " << getStrQueries() << std::endl
        << "- numberOfPreAllocatedBuffer: " << numberOfPreAllocatedBuffer->getValueAsString() << std::endl
        << "- numberOfBuffersToProduce: " << numberOfBuffersToProduce->getValueAsString() << std::endl
        << "- batchSize: " << batchSize->getValueAsString() << std::endl
        << "- dataProviderMode: " << dataProviderMode->getValue() << std::endl
        << "- connectionString: " << connectionString->getValue() << std::endl
        << "- logicalSources: " << getStrLogicalSrcDataGenerators() << std::endl
        << "- ingestionRateInBuffers: " << ingestionRateInBuffers->getValueAsString() << std::endl
        << "- ingestionRateCount: " << ingestionRateCount->getValueAsString() << std::endl
        << "- numberOfPeriods: " << numberOfPeriods->getValueAsString() << std::endl
        << "- ingestionRateDistribution: " << ingestionRateDistribution->getValue() << std::endl
        << "- customValues: " << customValues->getValue() << std::endl
        << "- joinStrategy: " << joinStrategy->getValue() << std::endl
        << "- dataProvider: " << dataProvider->getValue() << std::endl;

    return oss.str();
}

E2EBenchmarkConfigOverAllRuns E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(Yaml::Node yamlConfig) {
    E2EBenchmarkConfigOverAllRuns configOverAllRuns;

    configOverAllRuns.startupSleepIntervalInSeconds->setValueIfDefined(yamlConfig["startupSleepIntervalInSeconds"]);
    configOverAllRuns.numMeasurementsToCollect->setValueIfDefined(yamlConfig["numberOfMeasurementsToCollect"]);
    configOverAllRuns.experimentMeasureIntervalInSeconds->setValueIfDefined(yamlConfig["experimentMeasureIntervalInSeconds"]);
    configOverAllRuns.outputFile->setValueIfDefined(yamlConfig["outputFile"]);
    configOverAllRuns.benchmarkName->setValueIfDefined(yamlConfig["benchmarkName"]);
    configOverAllRuns.dataProviderMode->setValueIfDefined(yamlConfig["dataProviderMode"]);
    configOverAllRuns.joinStrategy->setValueIfDefined(yamlConfig["joinStrategy"]);
    configOverAllRuns.connectionString->setValueIfDefined(yamlConfig["connectionString"]);
    configOverAllRuns.inputType->setValueIfDefined(yamlConfig["inputType"]);
    configOverAllRuns.sourceSharing->setValueIfDefined(yamlConfig["sourceSharing"]);
    configOverAllRuns.numberOfPreAllocatedBuffer->setValueIfDefined(yamlConfig["numberOfPreAllocatedBuffer"]);
    configOverAllRuns.batchSize->setValueIfDefined(yamlConfig["batchSize"]);
    configOverAllRuns.numberOfBuffersToProduce->setValueIfDefined(yamlConfig["numberOfBuffersToProduce"]);

    auto dataProviderNode = yamlConfig["dataProvider"];
    if (!dataProviderNode.IsNone()) {
        configOverAllRuns.dataProvider->setValueIfDefined(dataProviderNode["name"]);
        configOverAllRuns.ingestionRateCount->setValueIfDefined(dataProviderNode["ingestionRateCount"]);

        auto ingestionRateDistributionNode = dataProviderNode["ingestionRateDistribution"];
        if (!ingestionRateDistributionNode.IsNone()) {
            configOverAllRuns.ingestionRateDistribution->setValueIfDefined(ingestionRateDistributionNode["type"]);
            configOverAllRuns.ingestionRateInBuffers->setValueIfDefined(ingestionRateDistributionNode["ingestionRateInBuffers"]);
            configOverAllRuns.numberOfPeriods->setValueIfDefined(ingestionRateDistributionNode["numberOfPeriods"]);
            configOverAllRuns.customValues->setValueIfDefined(ingestionRateDistributionNode["values"]);
        }
    }

    auto logicalSourcesNode = yamlConfig["logicalSources"];
    if (logicalSourcesNode.IsSequence()) {
        configOverAllRuns.sourceNameToDataGenerator.clear();
        for (auto entry = logicalSourcesNode.Begin(); entry != logicalSourcesNode.End(); entry++) {
            auto node = (*entry).second;
            auto sourceName = node["name"].As<std::string>();
            if (configOverAllRuns.sourceNameToDataGenerator.contains(sourceName)) {
                NES_THROW_RUNTIME_ERROR("Logical source name has to be unique. " << sourceName << " is not unique!");
            }

            configOverAllRuns.sourceNameToDataGenerator[sourceName] =
                DataGeneration::DataGenerator::createGeneratorByName(node["type"].As<std::string>(), node);
            ;
        }
        NES_DEBUG("No additional sources have been added!");
    }

    configOverAllRuns.queries.clear();
    if (!yamlConfig["query"].IsNone()) {
        configOverAllRuns.queries.push_back({yamlConfig["query"].As<std::string>(), defaultCustomDelayInSeconds});
    } else if (!yamlConfig["querySet"].IsNone() && yamlConfig["querySet"].Size() > 0) {
        Yaml::Node queriesNode = yamlConfig["querySet"];
        for (size_t i = 0; i < queriesNode.Size(); i++) {
            configOverAllRuns.queries.push_back({queriesNode[i]["query"].As<std::string>(), queriesNode[i]["customDelayInSeconds"].IsNone() ? 0 : queriesNode[i]["customDelayInSeconds"].As<uint32_t>()});
        }
    } else {
        NES_THROW_RUNTIME_ERROR("No query or queries defined!");
    }

    return configOverAllRuns;
}

std::string E2EBenchmarkConfigOverAllRuns::getStrLogicalSrcDataGenerators() const {
    std::stringstream stringStream;
    for (auto it = sourceNameToDataGenerator.begin(); it != sourceNameToDataGenerator.end(); ++it) {
        if (it != sourceNameToDataGenerator.begin()) {
            stringStream << ", ";
        }
        stringStream << it->first << ": " << it->second->getName();
    }

    return stringStream.str();
}

std::string E2EBenchmarkConfigOverAllRuns::getStrQueries() const {
    std::stringstream stringStream;
    for (auto it = queries.begin(); it != queries.end(); ++it) {
        if (it != queries.begin()) {
            stringStream << "\n";
        }
        stringStream << *it;
    }

    return stringStream.str();
}

size_t E2EBenchmarkConfigOverAllRuns::getTotalSchemaSize() {
    size_t size = 0;
    for (auto&& item : sourceNameToDataGenerator) {
        auto dataGenerator = item.second.get();
        size += dataGenerator->getSchema()->getSchemaSizeInBytes();
    }

    return size;
}
}// namespace NES::Benchmark