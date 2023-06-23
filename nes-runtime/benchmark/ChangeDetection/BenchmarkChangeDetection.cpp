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

#include <Execution/StatisticsCollector/ChangeDetectors/Adwin/Adwin.hpp>
#include <Execution/StatisticsCollector/ChangeDetectors/SeqDrift2/SeqDrift2.hpp>
#include <benchmark/benchmark.h>

namespace NES::Runtime::Execution {

static void BM_Adwin(benchmark::State& state) {
    // Perform setup here
    auto adwin = Adwin(0.1,5);
    auto value = 0.1;
    auto changeValue = 1.0;
    uint64_t i = 1;
    for (auto _ : state) {
        state.PauseTiming();
        if(i % state.range(0) == 0) {
            auto temp = value;
            value = changeValue;
            changeValue = temp;
        }
        i++;
        state.ResumeTiming();
        // This code gets timed
        adwin.insertValue(value);
    }
}

void BM_SeqDrift(benchmark::State& state) {
    // Perform setup here
    auto seqDrift2 = SeqDrift2(0.1,200);
    auto value = 0.1;
    auto changeValue = 1.0;
    uint64_t i = 1;
    for (auto _ : state) {
        state.PauseTiming();
        if(i % state.range(0) == 0) {
            auto temp = value;
            value = changeValue;
            changeValue = temp;
        }
        i++;
        state.ResumeTiming();
        // This code gets timed
        seqDrift2.insertValue(value);
    }
}

BENCHMARK(BM_Adwin)->Arg(1000)->Arg(10000)->Arg(100000)->Arg(10000000);
BENCHMARK(BM_SeqDrift)->Arg(1000)->Arg(10000)->Arg(100000)->Arg(1000000);

}// namespace NES::Runtime::Execution

BENCHMARK_MAIN();

