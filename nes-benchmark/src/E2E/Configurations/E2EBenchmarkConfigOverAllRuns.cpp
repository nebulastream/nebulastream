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
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>

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

        numSources = ConfigurationOption<uint32_t>::create("numSources", 1, "Number of sources");
        numberOfPreAllocatedBuffer = ConfigurationOption<uint32_t>::create("numberOfPreAllocatedBuffer", 1, "Pre-allocated buffer");
        outputFile = ConfigurationOption<std::string>::create("outputFile", "e2eBenchmarkRunner", "Filename of the output");
        benchmarkName = ConfigurationOption<std::string>::create("benchmarkName", "E2ERunner", "Name of the benchmark");
        inputType = ConfigurationOption<std::string>::create("inputType", "Auto", "How to read the input data");
        query = ConfigurationOption<std::string>::create("query", "", "Query to be run");
        dataProviderMode = ConfigurationOption<std::string>::create("dataProviderMode", "ZeroCopy", "DataProviderMode either ZeroCopy or MemCopy");
        dataGenerator = ConfigurationOption<std::string>::create("dataGenerator", "Default", "The source that provides the data");
        connectionString = ConfigurationOption<std::string>::create("connectionString", "", "Optional string to connect to source");
        logicalStreamName = ConfigurationOption<std::string>::create("logicalStreamName", "input", "Source stream name");


        for (auto sourceCnt = 0UL; sourceCnt < numSources->getValue(); ++sourceCnt) {
            std::string name = "input" + std::to_string(sourceCnt+1);
            dataGenerators[name] = DataGeneration::DataGenerator::createGeneratorByName("Default", Yaml::Node());
        }
    }
    std::string E2EBenchmarkConfigOverAllRuns::toString() {
        std::stringstream oss;
        oss << "- startupSleepIntervalInSeconds: " << startupSleepIntervalInSeconds->getValueAsString() << std::endl
            << "- numMeasurementsToCollect: " << numMeasurementsToCollect->getValueAsString() << std::endl
            << "- experimentMeasureIntervalInSeconds: " << experimentMeasureIntervalInSeconds->getValueAsString() << std::endl
            << "- outputFile: " << outputFile->getValue() << std::endl
            << "- benchmarkName: " << benchmarkName->getValue() << std::endl
            << "- inputType: " << inputType->getValue() << std::endl
            << "- query: " << query->getValue() << std::endl
            << "- numSources: " << numSources->getValueAsString() << std::endl
            << "- numberOfPreAllocatedBuffer: " << numberOfPreAllocatedBuffer->getValueAsString() << std::endl
            << "- dataProviderMode: " << dataProviderMode->getValue() << std::endl
            << "- dataGenerator: " << dataGenerator->getValue() << std::endl
            << "- connectionString: " << connectionString->getValue() << std::endl
            << "- logicalStreamName: " << logicalStreamName->getValue() << std::endl;
            << "- dataGenerations: " << getDataGeneratorsAsString() << std::endl
            << "- dataProviderMode: " << dataProviderMode->getValue() << std::endl;

        return oss.str();
    }

    E2EBenchmarkConfigOverAllRuns E2EBenchmarkConfigOverAllRuns::generateConfigOverAllRuns(Yaml::Node yamlConfig) {
        E2EBenchmarkConfigOverAllRuns configOverAllRuns;
        configOverAllRuns.startupSleepIntervalInSeconds->setValue(yamlConfig["startupSleepIntervalInSeconds"].As<uint32_t>());
        configOverAllRuns.numMeasurementsToCollect->setValue(yamlConfig["numberOfMeasurementsToCollect"].As<uint32_t>());
        configOverAllRuns.experimentMeasureIntervalInSeconds->setValue(yamlConfig["experimentMeasureIntervalInSeconds"].As<uint32_t>());
        configOverAllRuns.outputFile->setValue(yamlConfig["outputFile"].As<std::string>());
        configOverAllRuns.benchmarkName->setValue(yamlConfig["benchmarkName"].As<std::string>());
        configOverAllRuns.query->setValue(yamlConfig["query"].As<std::string>());
        configOverAllRuns.dataProviderMode->setValue(yamlConfig["dataProviderMode"].As<std::string>());
        configOverAllRuns.dataGenerator->setValue(yamlConfig["dataGenerator"].As<std::string>());
        configOverAllRuns.connectionString->setValue(yamlConfig["connectionString"].As<std::string>());
        configOverAllRuns.logicalStreamName->setValue(yamlConfig["logicalStreamName"].As<std::string>());
        configOverAllRuns.numSources->setValue(yamlConfig["numberOfSources"].As<uint32_t>());
        configOverAllRuns.numberOfPreAllocatedBuffer->setValue(yamlConfig["numberOfPreAllocatedBuffer"].As<uint32_t>());

        if (yamlConfig["dataGenerators"].Size() > 0) {
            /* Iterating through the node
             * Afterwards, we insert either the parsed values or Default, if we require another generator
             */
            configOverAllRuns.dataGenerators.clear();
            configOverAllRuns.dataGenerators.reserve(configOverAllRuns.numSources->getValue());

            NES_INFO("Creating new data generators as specified in the config!");

            auto dataGenerators = yamlConfig["dataGenerators"];
            for (auto it = dataGenerators.Begin(); it != dataGenerators.End(); it++) {
                std::string name = (*it).first;
                Yaml::Node node = (*it).second;

                std::string generatorName = node["type"].As<std::string>();
                configOverAllRuns.dataGenerators[name] = DataGeneration::DataGenerator::createGeneratorByName(generatorName, node);
            }
        }

        // Padding the data generators to fit the number of sources. Creating a default data generator
        for (auto sourceCnt = configOverAllRuns.dataGenerators.size(); sourceCnt < configOverAllRuns.numSources->getValue(); ++sourceCnt) {
            std::string name = "input" + std::to_string(sourceCnt+1);
            configOverAllRuns.dataGenerators[name] = DataGeneration::DataGenerator::createGeneratorByName("Default", Yaml::Node());
        }

        return configOverAllRuns;
    }
    std::string E2EBenchmarkConfigOverAllRuns::getDataGeneratorsAsString() {
        std::ostringstream oss;

        auto cnt = 0;
        for (auto& item : dataGenerators) {
            if (cnt != 0) {
                oss << ", ";
            }
            oss << item.second->toString();
            cnt += 1;
        }

        return oss.str();
    }
}