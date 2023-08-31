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

#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <Util/BenchmarkUtils.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::Benchmark {
E2EBenchmarkConfigPerRun::E2EBenchmarkConfigPerRun() {
    using namespace Configurations;
    numberOfWorkerThreads = ConfigurationOption<uint32_t>::create("numWorkerOfThreads", 1, "No. Worker Threads");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 1024, "Buffer size in bytes");
    numberOfQueriesToDeploy = ConfigurationOption<uint32_t>::create("numberOfQueriesToDeploy", 1, "Number of Queries to use");
    numberOfBuffersInGlobalBufferManager =
        ConfigurationOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Overall buffer count");
    numberOfBuffersInSourceLocalBufferPool =
        ConfigurationOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool", 128, "Buffer per source");
    pageSize = ConfigurationOption<uint32_t>::create("pageSize", 4096, "pageSize in HT");
    preAllocPageCnt = ConfigurationOption<uint32_t>::create("preAllocPageCnt", 1, "preAllocPageCnt in Bucket");
    numberOfPartitions = ConfigurationOption<uint32_t>::create("numberOfPartitions", 1, "numberOfPartitions in HT");
    maxHashTableSize = ConfigurationOption<uint64_t>::create("maxHashTableSize", 0, "max hash table size");
    query = ConfigurationOption<std::string>::create("query", "", "query to execute");
    windowingStrategy = ConfigurationOption<std::string>::create("windowingStrategy", "Slicing", "windowingStrategy");

    logicalSrcToNoPhysicalSrc = {{"input1", 1}};
}

std::string E2EBenchmarkConfigPerRun::toString() {
    std::stringstream oss;
    oss << "- numWorkerOfThreads: " << numberOfWorkerThreads->getValueAsString() << std::endl
        << "- bufferSizeInBytes: " << bufferSizeInBytes->getValueAsString() << std::endl
        << "- numberOfQueriesToDeploy: " << numberOfQueriesToDeploy->getValueAsString() << std::endl
        << "- numberOfSources: " << getStringLogicalSourceToNumberOfPhysicalSources() << std::endl
        << "- numberOfBuffersInGlobalBufferManager: " << numberOfBuffersInGlobalBufferManager->getValueAsString() << std::endl
        << "- numberOfBuffersInSourceLocalBufferPool: " << numberOfBuffersInSourceLocalBufferPool->getValueAsString() << std::endl
        << "- pageSize: " << pageSize->getValueAsString() << std::endl
        << "- preAllocPageCnt: " << preAllocPageCnt->getValueAsString() << std::endl
        << "- numberOfPartitions: " << numberOfPartitions->getValueAsString() << std::endl
        << "- maxHashTableSize: " << maxHashTableSize->getValueAsString() << std::endl
        << "- query: " << query->getValue() << std::endl
        << "- windowingStrategy: " << windowingStrategy->getValue() << std::endl;

    std::cout << oss.str() << std::endl;
    return oss.str();
}

std::vector<E2EBenchmarkConfigPerRun> E2EBenchmarkConfigPerRun::generateAllConfigsPerRun(Yaml::Node yamlConfig) {
    std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;
    E2EBenchmarkConfigPerRun configPerRun;

    /* Getting all parameters per experiment run in vectors */
    auto numWorkerOfThreads = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfWorkerThreads"].As<std::string>(),
                                                                  configPerRun.numberOfWorkerThreads->getDefaultValue());

    auto bufferSizeInBytes = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["bufferSizeInBytes"].As<std::string>(),
                                                                 configPerRun.bufferSizeInBytes->getDefaultValue());
    auto numberOfQueriesToDeploy = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfQueriesToDeploy"].As<std::string>(),
                                                                       configPerRun.numberOfQueriesToDeploy->getDefaultValue());

    auto numberOfBuffersInGlobalBufferManager =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInGlobalBufferManager"].As<std::string>(),
                                            configPerRun.numberOfBuffersInGlobalBufferManager->getDefaultValue());

    auto numberOfBuffersInSourceLocalBufferPool =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInSourceLocalBufferPool"].As<std::string>(),
                                            configPerRun.numberOfBuffersInSourceLocalBufferPool->getDefaultValue());
    auto pageSizes =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["pageSize"].As<std::string>(), configPerRun.pageSize->getDefaultValue());
    auto preAllocPageCnts = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["preAllocPageCnt"].As<std::string>(),
                                                                configPerRun.preAllocPageCnt->getDefaultValue());
    auto numberOfPartitions = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfPartitions"].As<std::string>(),
                                                                  configPerRun.numberOfPartitions->getDefaultValue());

    auto maxHashTableSizes = Util::splitAndFillIfEmpty<uint64_t>(yamlConfig["maxHashTableSize"].As<std::string>(),
                                                                 configPerRun.maxHashTableSize->getDefaultValue());

    auto queries = Util::splitString(yamlConfig["query"].As<std::string>(), configPerRun.query->getDefaultValue(), "@");
    auto windowingStrategies = Util::splitString(yamlConfig["windowingStrategy"].As<std::string>(), configPerRun.windowingStrategy->getDefaultValue(), ",");

    std::vector<std::map<std::string, uint64_t>> allLogicalSrcToPhysicalSources = {configPerRun.logicalSrcToNoPhysicalSrc};
    if (yamlConfig["logicalSources"].IsNone()) {
        NES_THROW_RUNTIME_ERROR("logicalSources could not been found in the yaml config file!");
    }
    allLogicalSrcToPhysicalSources = E2EBenchmarkConfigPerRun::generateMapsLogicalSrcToNumberOfPhysicalSources(yamlConfig);

    /* Retrieving the maximum number of experiments to run */
    size_t totalBenchmarkRuns = numWorkerOfThreads.size();
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, bufferSizeInBytes.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfQueriesToDeploy.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInGlobalBufferManager.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInSourceLocalBufferPool.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, pageSizes.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, preAllocPageCnts.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfPartitions.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, maxHashTableSizes.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, allLogicalSrcToPhysicalSources.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, queries.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, windowingStrategies.size());

    /* Padding all vectors to the desired size */
    Util::padVectorToSize<uint32_t>(numWorkerOfThreads, totalBenchmarkRuns, numWorkerOfThreads.back());
    Util::padVectorToSize<uint32_t>(bufferSizeInBytes, totalBenchmarkRuns, bufferSizeInBytes.back());
    Util::padVectorToSize<uint32_t>(numberOfQueriesToDeploy, totalBenchmarkRuns, numberOfQueriesToDeploy.back());
    Util::padVectorToSize<uint32_t>(numberOfBuffersInGlobalBufferManager,
                                    totalBenchmarkRuns,
                                    numberOfBuffersInGlobalBufferManager.back());
    Util::padVectorToSize<uint32_t>(numberOfBuffersInSourceLocalBufferPool,
                                    totalBenchmarkRuns,
                                    numberOfBuffersInSourceLocalBufferPool.back());
    Util::padVectorToSize<uint32_t>(pageSizes, totalBenchmarkRuns, pageSizes.back());
    Util::padVectorToSize<uint32_t>(preAllocPageCnts, totalBenchmarkRuns, preAllocPageCnts.back());
    Util::padVectorToSize<uint32_t>(numberOfPartitions, totalBenchmarkRuns, numberOfPartitions.back());
    Util::padVectorToSize<uint64_t>(maxHashTableSizes, totalBenchmarkRuns, maxHashTableSizes.back());
    Util::padVectorToSize<std::string>(queries, totalBenchmarkRuns, queries.back());
    Util::padVectorToSize<std::string>(windowingStrategies, totalBenchmarkRuns, windowingStrategies.back());
    Util::padVectorToSize<std::map<std::string, uint64_t>>(allLogicalSrcToPhysicalSources,
                                                           totalBenchmarkRuns,
                                                           allLogicalSrcToPhysicalSources.back());

    allConfigPerRuns.reserve(totalBenchmarkRuns);
    for (size_t i = 0; i < totalBenchmarkRuns; ++i) {
        E2EBenchmarkConfigPerRun e2EBenchmarkConfigPerRun;
        e2EBenchmarkConfigPerRun.numberOfWorkerThreads->setValue(numWorkerOfThreads[i]);
        e2EBenchmarkConfigPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes[i]);
        e2EBenchmarkConfigPerRun.numberOfQueriesToDeploy->setValue(numberOfQueriesToDeploy[i]);
        e2EBenchmarkConfigPerRun.numberOfBuffersInGlobalBufferManager->setValue(numberOfBuffersInGlobalBufferManager[i]);
        e2EBenchmarkConfigPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(numberOfBuffersInSourceLocalBufferPool[i]);
        e2EBenchmarkConfigPerRun.pageSize->setValue(pageSizes[i]);
        e2EBenchmarkConfigPerRun.preAllocPageCnt->setValue(preAllocPageCnts[i]);
        e2EBenchmarkConfigPerRun.numberOfPartitions->setValue(numberOfPartitions[i]);
        e2EBenchmarkConfigPerRun.maxHashTableSize->setValue(maxHashTableSizes[i]);
        e2EBenchmarkConfigPerRun.logicalSrcToNoPhysicalSrc = allLogicalSrcToPhysicalSources[i];
        e2EBenchmarkConfigPerRun.query->setValue(queries[i]);
        e2EBenchmarkConfigPerRun.windowingStrategy->setValue(windowingStrategies[i]);

        allConfigPerRuns.push_back(e2EBenchmarkConfigPerRun);
    }

    return allConfigPerRuns;
}

std::string E2EBenchmarkConfigPerRun::getStringLogicalSourceToNumberOfPhysicalSources() const {
    std::stringstream stringStream;
    for (auto it = logicalSrcToNoPhysicalSrc.begin(); it != logicalSrcToNoPhysicalSrc.end(); ++it) {
        if (it != logicalSrcToNoPhysicalSrc.begin()) {
            stringStream << ", ";
        }

        stringStream << it->first << ": " << it->second;
    }

    return stringStream.str();
}

std::vector<std::map<std::string, uint64_t>>
E2EBenchmarkConfigPerRun::generateMapsLogicalSrcToNumberOfPhysicalSources(Yaml::Node yamlConfig) {
    std::vector<std::map<std::string, uint64_t>> retVectorOfMaps;

    auto logicalSourceNode = yamlConfig["logicalSources"];
    auto maxNumberOfExperiments = 1UL;

    // Iterating through all numberOfPhysicalSources and getting the maximum number of experiments (most comma separated values)
    for (auto entry = logicalSourceNode.Begin(); entry != logicalSourceNode.End(); entry++) {
        auto node = (*entry).second;
        if (!node["numberOfPhysicalSources"].IsNone()) {
            auto tmpVec = NES::Util::splitWithStringDelimiter<uint64_t>(node["numberOfPhysicalSources"].As<std::string>(), ",");
            maxNumberOfExperiments = std::max(maxNumberOfExperiments, tmpVec.size());
        }
    }

    /* Iterating through each source and retrieving the number of physical sources.
     * First, we check if the logical source name already exists and throw an error if so.
     * Afterwards, we take either the old numberOfPhysicalSources, or the new value if it exists. If none exist, we have a default of 1.
     */
    for (auto curExp = 0UL; curExp < maxNumberOfExperiments; ++curExp) {
        std::map<std::string, uint64_t> map;
        for (auto entry = logicalSourceNode.Begin(); entry != logicalSourceNode.End(); entry++) {
            auto node = (*entry).second;
            auto logicalSourceName = node["name"].As<std::string>();
            if (map.contains(logicalSourceName)) {
                NES_THROW_RUNTIME_ERROR("Logical source name has to be unique! " << logicalSourceName << " is duplicated!");
            }

            auto value = 1UL;
            auto tmpVec = NES::Util::splitWithStringDelimiter<uint64_t>(node["numberOfPhysicalSources"].As<std::string>(), ",");
            if (!node["numberOfPhysicalSources"].IsNone() && curExp < tmpVec.size()) {
                value = tmpVec[curExp];
            } else if (!retVectorOfMaps.empty()) {
                auto& lastMap = retVectorOfMaps[curExp - 1];
                if (lastMap.contains(logicalSourceName)) {
                    value = lastMap[logicalSourceName];
                }
            }

            map[logicalSourceName] = value;
        }
        retVectorOfMaps.emplace_back(map);
    }

    return retVectorOfMaps;
}
}// namespace NES::Benchmark