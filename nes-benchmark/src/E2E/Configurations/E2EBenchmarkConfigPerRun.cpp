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

#include <Util/BenchmarkUtils.hpp>

#include <E2E/Configurations/E2EBenchmarkConfigPerRun.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::Benchmark {
E2EBenchmarkConfigPerRun::E2EBenchmarkConfigPerRun() {
    using namespace Configurations;
    numWorkerOfThreads = ConfigurationOption<uint32_t>::create("numWorkerOfThreads", 1, "No. Worker Threads");
    numberOfSources = ConfigurationOption<uint32_t>::create("numberOfSources", 1, "No. sources");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 1024, "Buffer size in bytes");
    numberOfQueriesToDeploy = ConfigurationOption<uint32_t>::create("numberOfQueriesToDeploy", 1, "Number of Queries to use");
    numberOfBuffersInGlobalBufferManager =
        ConfigurationOption<uint32_t>::create("numberOfBuffersInGlobalBufferManager", 1024, "Overall buffer count");
    numberOfBuffersPerPipeline = ConfigurationOption<uint32_t>::create("numberOfBuffersPerPipeline", 128, "Buffer per pipeline");
    numberOfBuffersInSourceLocalBufferPool =
        ConfigurationOption<uint32_t>::create("numberOfBuffersInSourceLocalBufferPool", 128, "Buffer per source");
}

std::string E2EBenchmarkConfigPerRun::toString() {
    std::stringstream oss;
    oss << "- numWorkerOfThreads: " << numWorkerOfThreads->getValueAsString() << std::endl
        << "- bufferSizeInBytes: " << bufferSizeInBytes->getValueAsString() << std::endl
        << "- numberOfQueriesToDeploy: " << numberOfQueriesToDeploy->getValueAsString() << std::endl
        << "- numberOfSources: " << numberOfSources->getValueAsString() << std::endl
        << "- numberOfBuffersInGlobalBufferManager: " << numberOfBuffersInGlobalBufferManager->getValueAsString() << std::endl
        << "- numberOfBuffersPerPipeline: " << numberOfBuffersPerPipeline->getValueAsString() << std::endl
        << "- numberOfBuffersInSourceLocalBufferPool: " << numberOfBuffersInSourceLocalBufferPool->getValueAsString()
        << std::endl;

    return oss.str();
}

std::vector<E2EBenchmarkConfigPerRun> E2EBenchmarkConfigPerRun::generateAllConfigsPerRun(Yaml::Node yamlConfig) {
    std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;

    E2EBenchmarkConfigPerRun configPerRun;

    /* Getting all parameters per experiment run in vectors */
    auto numWorkerOfThreads = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfWorkerThreads"].As<std::string>(),
                                                                  configPerRun.numWorkerOfThreads->getDefaultValue());

    auto numberOfSources = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfSources"].As<std::string>(),
                                                               configPerRun.numberOfSources->getDefaultValue());

    auto bufferSizeInBytes = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["bufferSizeInBytes"].As<std::string>(),
                                                                 configPerRun.bufferSizeInBytes->getDefaultValue());
    auto numberOfQueriesToDeploy = Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfQueriesToDeploy"].As<std::string>(),
                                                                       configPerRun.numberOfQueriesToDeploy->getDefaultValue());

    auto numberOfBuffersInGlobalBufferManager =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInGlobalBufferManager"].As<std::string>(),
                                            configPerRun.numberOfBuffersInGlobalBufferManager->getDefaultValue());

    auto numberOfBuffersPerPipeline =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersPerPipeline"].As<std::string>(),
                                            configPerRun.numberOfBuffersPerPipeline->getDefaultValue());

    auto numberOfBuffersInSourceLocalBufferPool =
        Util::splitAndFillIfEmpty<uint32_t>(yamlConfig["numberOfBuffersInSourceLocalBufferPool"].As<std::string>(),
                                            configPerRun.numberOfBuffersInSourceLocalBufferPool->getDefaultValue());

    /* Retrieving the maximum number of experiments to run */
    size_t totalBenchmarkRuns = numWorkerOfThreads.size();
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, bufferSizeInBytes.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfQueriesToDeploy.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInGlobalBufferManager.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersPerPipeline.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfBuffersInSourceLocalBufferPool.size());
    totalBenchmarkRuns = std::max(totalBenchmarkRuns, numberOfSources.size());

    /* Padding all vectors to the desired size */
    Util::padVectorToSize<uint32_t>(numWorkerOfThreads, totalBenchmarkRuns, numWorkerOfThreads.back());
    Util::padVectorToSize<uint32_t>(numberOfSources, totalBenchmarkRuns, numberOfSources.back());
    Util::padVectorToSize<uint32_t>(bufferSizeInBytes, totalBenchmarkRuns, bufferSizeInBytes.back());
    Util::padVectorToSize<uint32_t>(numberOfQueriesToDeploy, totalBenchmarkRuns, numberOfQueriesToDeploy.back());
    Util::padVectorToSize<uint32_t>(numberOfBuffersInGlobalBufferManager,
                                    totalBenchmarkRuns,
                                    numberOfBuffersInGlobalBufferManager.back());
    Util::padVectorToSize<uint32_t>(numberOfBuffersPerPipeline, totalBenchmarkRuns, numberOfBuffersPerPipeline.back());
    Util::padVectorToSize<uint32_t>(numberOfBuffersInSourceLocalBufferPool,
                                    totalBenchmarkRuns,
                                    numberOfBuffersInSourceLocalBufferPool.back());

    allConfigPerRuns.reserve(totalBenchmarkRuns);
    for (size_t i = 0; i < totalBenchmarkRuns; ++i) {
        E2EBenchmarkConfigPerRun e2EBenchmarkConfigPerRun;
        e2EBenchmarkConfigPerRun.numWorkerOfThreads->setValue(numWorkerOfThreads[i]);
        e2EBenchmarkConfigPerRun.numberOfSources->setValue(numberOfSources[i]);
        e2EBenchmarkConfigPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes[i]);
        e2EBenchmarkConfigPerRun.numberOfQueriesToDeploy->setValue(numberOfQueriesToDeploy[i]);

        e2EBenchmarkConfigPerRun.numberOfBuffersInGlobalBufferManager->setValue(numberOfBuffersInGlobalBufferManager[i]);
        e2EBenchmarkConfigPerRun.numberOfBuffersPerPipeline->setValue(numberOfBuffersPerPipeline[i]);
        e2EBenchmarkConfigPerRun.numberOfBuffersInSourceLocalBufferPool->setValue(numberOfBuffersInSourceLocalBufferPool[i]);

        allConfigPerRuns.push_back(e2EBenchmarkConfigPerRun);
    }

    return allConfigPerRuns;
}
}// namespace NES::Benchmark