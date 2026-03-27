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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/CRC32HashFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/Hash/SipHashFunction.hpp>
#include <Nautilus/Interface/Hash/XXH3HashFunction.hpp>
#include <benchmark/benchmark.h>
#include <val_details.hpp>

namespace NES
{

/// Helper to extract the raw uint64_t from a nautilus hash value.
static uint64_t extractHash(const HashFunction::HashValue& hashValue)
{
    return nautilus::details::RawValueResolver<uint64_t>::getRawValue(hashValue);
}

/// Benchmark hashing N uint64 values with a given hash function.
template <typename HashFunctionType>
static void benchmarkHashUInt64(benchmark::State& state)
{
    const auto numValues = static_cast<uint64_t>(state.range(0));
    HashFunctionType hashFunction;

    /// Pre-create VarVal objects.
    std::vector<VarVal> values;
    values.reserve(numValues);
    for (uint64_t i = 0; i < numValues; ++i)
    {
        values.emplace_back(nautilus::val<uint64_t>(i * 0xDEADBEEF + 42));
    }

    for (auto _ : state)
    {
        uint64_t sum = 0;
        for (const auto& val : values)
        {
            sum += extractHash(hashFunction.calculate(val));
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues) * static_cast<int64_t>(sizeof(uint64_t)));
}

/// Benchmark hashing N int32 values.
template <typename HashFunctionType>
static void benchmarkHashInt32(benchmark::State& state)
{
    const auto numValues = static_cast<int32_t>(state.range(0));
    HashFunctionType hashFunction;

    std::vector<VarVal> values;
    values.reserve(numValues);
    for (int32_t i = 0; i < numValues; ++i)
    {
        values.emplace_back(nautilus::val<int32_t>(i * 7 + 13));
    }

    for (auto _ : state)
    {
        uint64_t sum = 0;
        for (const auto& val : values)
        {
            sum += extractHash(hashFunction.calculate(val));
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues) * static_cast<int64_t>(sizeof(int32_t)));
}

/// Benchmark hashing N double values.
template <typename HashFunctionType>
static void benchmarkHashDouble(benchmark::State& state)
{
    const auto numValues = static_cast<uint64_t>(state.range(0));
    HashFunctionType hashFunction;

    std::vector<VarVal> values;
    values.reserve(numValues);
    for (uint64_t i = 0; i < numValues; ++i)
    {
        values.emplace_back(nautilus::val<double>(static_cast<double>(i) * 3.14159));
    }

    for (auto _ : state)
    {
        uint64_t sum = 0;
        for (const auto& val : values)
        {
            sum += extractHash(hashFunction.calculate(val));
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(numValues) * static_cast<int64_t>(sizeof(double)));
}

/// Benchmark hashing multiple values at once (multi-key hashing).
template <typename HashFunctionType>
static void benchmarkHashMultiKey(benchmark::State& state)
{
    const auto numKeys = static_cast<uint64_t>(state.range(0));
    HashFunctionType hashFunction;

    /// Create a "row" of numKeys values.
    std::vector<VarVal> row;
    row.reserve(numKeys);
    for (uint64_t i = 0; i < numKeys; ++i)
    {
        row.emplace_back(nautilus::val<uint64_t>(i * 0xCAFEBABE + 7));
    }

    for (auto _ : state)
    {
        auto result = extractHash(hashFunction.calculate(row));
        benchmark::DoNotOptimize(result);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

/// --- uint64 benchmarks ---
BENCHMARK(benchmarkHashUInt64<MurMur3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("MurMur3/uint64");
BENCHMARK(benchmarkHashUInt64<CRC32HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("CRC32/uint64");
BENCHMARK(benchmarkHashUInt64<XXH3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("XXH3/uint64");
BENCHMARK(benchmarkHashUInt64<SipHashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("SipHash/uint64");

/// --- int32 benchmarks ---
BENCHMARK(benchmarkHashInt32<MurMur3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("MurMur3/int32");
BENCHMARK(benchmarkHashInt32<CRC32HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("CRC32/int32");
BENCHMARK(benchmarkHashInt32<XXH3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("XXH3/int32");
BENCHMARK(benchmarkHashInt32<SipHashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("SipHash/int32");

/// --- double benchmarks ---
BENCHMARK(benchmarkHashDouble<MurMur3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("MurMur3/double");
BENCHMARK(benchmarkHashDouble<CRC32HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("CRC32/double");
BENCHMARK(benchmarkHashDouble<XXH3HashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("XXH3/double");
BENCHMARK(benchmarkHashDouble<SipHashFunction>)->RangeMultiplier(10)->Range(100, 1000000)->Name("SipHash/double");

/// --- multi-key benchmarks (1 to 8 keys of uint64) ---
BENCHMARK(benchmarkHashMultiKey<MurMur3HashFunction>)->DenseRange(1, 8)->Name("MurMur3/multikey");
BENCHMARK(benchmarkHashMultiKey<CRC32HashFunction>)->DenseRange(1, 8)->Name("CRC32/multikey");
BENCHMARK(benchmarkHashMultiKey<XXH3HashFunction>)->DenseRange(1, 8)->Name("XXH3/multikey");
BENCHMARK(benchmarkHashMultiKey<SipHashFunction>)->DenseRange(1, 8)->Name("SipHash/multikey");

}

BENCHMARK_MAIN();
