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

#include <E2EBenchmarkConfigPerRun.hpp>
NES::Benchmark::E2EBenchmarkConfigPerRun::E2EBenchmarkConfigPerRun()  {
    using namespace Configurations;
    numWorkerThreads = ConfigurationOption<uint32_t>::create("numWorkerThreads", 1, "No. Worker Threads");
    numSources = ConfigurationOption<uint32_t>::create("numSources", 1, "No. source");

    numBuffersToProduce = ConfigurationOption<uint32_t>::create("numBuffersToProduce", 5000000, "No. buffers to produce");
    bufferSizeInBytes = ConfigurationOption<uint32_t>::create("bufferSizeInBytes", 1024, "Buffer size in bytes");
}

std::string NES::Benchmark::E2EBenchmarkConfigPerRun::toString()  {
    std::stringstream oss;
    oss << "- numWorkerThreads: " << numWorkerThreads<< std::endl
        << "- numSources: " << numSources << std::endl

        << "- numBuffersToProduce: " << numBuffersToProduce << std::endl
        << "- bufferSizeInBytes: " << bufferSizeInBytes << std::endl;

    return oss.str();
}
