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
#include <chrono>
#include <thread>
#include <benchmark/benchmark.h>
namespace NES {

static void BM_Test123(benchmark::State& state)
{
    for (auto _ : state) {
        /// sleeping randomly via rand()
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 100));
    }
}

}

/// Registering all benchmark functions
BENCHMARK(NES::BM_Test123);

BENCHMARK_MAIN();