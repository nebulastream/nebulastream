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
#include <Runtime/BufferManager.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <benchmark/benchmark.h>

using NESSchema = NES::Schema;
namespace NES::Unikernel {

static void BM_Floats(benchmark::State& state) {
    using schema = Schema<Field<FLOAT32>, Field<FLOAT32>, Field<INT64>, Field<FLOAT64>, Field<FLOAT64>>;
    auto bufferManager = Runtime::BufferManager(8192);
    auto buffer = bufferManager.getBufferBlocking();
    auto schemaBuffer = SchemaBuffer<schema, 8192>::of(buffer);
    const std::string v = "2345.4,567567.33,-2134234,12,0.4e23\n";
    for (auto _ : state) {
        parseCSVIntoBuffer(v, schemaBuffer);
        schemaBuffer.getBuffer().setNumberOfTuples(0);
    }
}

static void BM_FiveInts(benchmark::State& state) {
    using schema = Schema<Field<INT64>, Field<INT64>, Field<INT64>, Field<INT64>, Field<INT64>>;
    auto bufferManager = Runtime::BufferManager(8192);
    auto buffer = bufferManager.getBufferBlocking();
    auto schemaBuffer = SchemaBuffer<schema, 8192>::of(buffer);
    const std::string v = "2345,567567,-2134234,345345,12\n";
    for (auto _ : state) {
        parseCSVIntoBuffer(v, schemaBuffer);
        schemaBuffer.getBuffer().setNumberOfTuples(0);
    }
}

BENCHMARK(BM_FiveInts);
BENCHMARK(BM_Floats);
}// namespace NES::Unikernel
BENCHMARK_MAIN();
