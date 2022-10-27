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
#include <Util/UtilityFunctions.hpp>
#include <Util/yaml/Yaml.hpp>

namespace NES::Benchmark {
    E2EBenchmarkConfigPerRun::E2EBenchmarkConfigPerRun()  {
        using namespace Configurations;
        numWorkerThreads = ConfigurationOption<uint32_t>::create("numWorkerThreads", 1, "No. Worker Threads");
        numBuffersToProduce = ConfigurationOption<uint32_t>::create("numBuffersToProduce", 5000000, "No. buffers to produce");
        bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 1024, "Buffer size in bytes");
    }

    std::string E2EBenchmarkConfigPerRun::toString()  {
        std::stringstream oss;
        oss << "- numWorkerThreads: " << numWorkerThreads->getValueAsString() << std::endl
            << "- numBuffersToProduce: " << numBuffersToProduce->getValueAsString() << std::endl
            << "- bufferSizeInBytes: " << bufferSizeInBytes->getValueAsString() << std::endl;

        return oss.str();
    }

    std::vector<E2EBenchmarkConfigPerRun> E2EBenchmarkConfigPerRun::generateAllConfigsPerRun(Yaml::Node yamlConfig) {
        std::vector<E2EBenchmarkConfigPerRun> allConfigPerRuns;

        /* Getting all parameters per experiment run in vectors */
        auto numWorkerThreads = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["numberOfWorkerThreads"].As<std::string>(), ",");
        auto numBuffersToProduce = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["numberOfBuffersToProduce"].As<std::string>(), ",");
        auto bufferSizeInBytes = Util::splitWithStringDelimiter<uint32_t>(yamlConfig["bufferSizeInBytes"].As<std::string>(), ",");

        size_t totalBenchmarkRuns = numWorkerThreads.size();
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, numBuffersToProduce.size());
        totalBenchmarkRuns = std::max(totalBenchmarkRuns, bufferSizeInBytes.size());

        Util::padVectorToSize<uint32_t>(numWorkerThreads, totalBenchmarkRuns, numWorkerThreads.back());
        Util::padVectorToSize<uint32_t>(numBuffersToProduce, totalBenchmarkRuns, numBuffersToProduce.back());
        Util::padVectorToSize<uint32_t>(bufferSizeInBytes, totalBenchmarkRuns, bufferSizeInBytes.back());

        allConfigPerRuns.reserve(totalBenchmarkRuns);

        for (size_t i = 0; i < numBuffersToProduce.size(); ++i) {
            E2EBenchmarkConfigPerRun e2EBenchmarkConfigPerRun;
            e2EBenchmarkConfigPerRun.numWorkerThreads->setValue(numWorkerThreads[i]);
            e2EBenchmarkConfigPerRun.numBuffersToProduce->setValue(numBuffersToProduce[i]);
            e2EBenchmarkConfigPerRun.bufferSizeInBytes->setValue(bufferSizeInBytes[i]);

            allConfigPerRuns.push_back(e2EBenchmarkConfigPerRun);
        }

        return allConfigPerRuns;
    }
}