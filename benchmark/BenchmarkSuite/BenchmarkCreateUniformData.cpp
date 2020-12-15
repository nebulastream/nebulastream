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

#include <benchmark/benchmark.h>
#include <util/BenchmarkUtils.hpp>

static std::string fileName = "create_uniform_data";

using namespace NES::Benchmarking;
static void BM_CreateUniformData(benchmark::State& state) {

    uint64_t curNumberOfTuplesPerBuffer = state.range(0);

    std::list<uint64_t> keyList;
    BenchmarkUtils::createUniformData(keyList, curNumberOfTuplesPerBuffer);

    uint64_t tmpSum = 0;
    for (auto it : keyList) {
        tmpSum += it;
    }
    double mean = (double)tmpSum / keyList.size();

    tmpSum = 0;
    for (auto it : keyList) {
        tmpSum += std::pow(it - mean, 2);
    }
    double stddev = std::sqrt((double)tmpSum / keyList.size());

    auto t = std::time(nullptr);
    auto tm = *std::localtime(&t);

    std::ostringstream oss;
    oss << std::put_time(&tm, "%d-%m-%Y_%H-%M-%S");

    std::ofstream file;
    file.open((fileName) + "_results.csv", std::ios_base::app);
    file << oss.str() << "," << mean << "," << stddev;
    file.close();
}

// Register benchmark
BENCHMARK(BM_CreateUniformData)
    ->DenseRange(100, 10000, 100)
    ->Repetitions(1e8);

// A benchmark main is needed
int main(int argc, char** argv) {

    std::ofstream file;
    file.open((fileName) + "_results.csv", std::ios_base::app);
    file << "Time, NumberOfTuplesPerBuffer, Mean, Stddev\n";
    file.close();

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    return 0;
}