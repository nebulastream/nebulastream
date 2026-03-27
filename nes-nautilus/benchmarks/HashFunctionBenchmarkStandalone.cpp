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
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/CRC32HashFunction.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/Hash/MurMur3HashFunction.hpp>
#include <Nautilus/Interface/Hash/SipHashFunction.hpp>
#include <Nautilus/Interface/Hash/XXH3HashFunction.hpp>
#include <val_details.hpp>

namespace NES
{

using Clock = std::chrono::high_resolution_clock;

static uint64_t extractHash(const HashFunction::HashValue& hashValue)
{
    return nautilus::details::RawValueResolver<uint64_t>::getRawValue(hashValue);
}

struct BenchResult
{
    std::string name;
    std::string dataType;
    uint64_t numValues;
    double totalNs;
    double nsPerHash;
    double mHashesPerSec;
};

/// Warm up, then measure. Returns nanoseconds for the timed portion.
template <typename Func>
static double measure(Func&& fn, int warmupRuns, int timedRuns)
{
    for (int i = 0; i < warmupRuns; ++i)
    {
        fn();
    }

    auto t0 = Clock::now();
    for (int i = 0; i < timedRuns; ++i)
    {
        fn();
    }
    auto t1 = Clock::now();
    return std::chrono::duration<double, std::nano>(t1 - t0).count() / timedRuns;
}

static BenchResult benchmarkSingleValue(const std::string& hashName, HashFunction& hashFunction, const std::string& typeName,
                                        const std::vector<VarVal>& values, int timedRuns)
{
    const auto numValues = values.size();

    volatile uint64_t sink = 0;
    auto fn = [&]()
    {
        uint64_t sum = 0;
        for (const auto& val : values)
        {
            sum += extractHash(hashFunction.calculate(val));
        }
        sink = sum;
    };

    const double totalNs = measure(fn, 2, timedRuns);
    const double nsPerHash = totalNs / static_cast<double>(numValues);
    const double mHashesPerSec = static_cast<double>(numValues) / totalNs * 1e3;

    return {hashName, typeName, numValues, totalNs, nsPerHash, mHashesPerSec};
}

static BenchResult benchmarkMultiKey(const std::string& hashName, HashFunction& hashFunction, uint64_t numKeys, int timedRuns)
{
    std::vector<VarVal> row;
    row.reserve(numKeys);
    for (uint64_t i = 0; i < numKeys; ++i)
    {
        row.emplace_back(nautilus::val<uint64_t>(i * 0xCAFEBABE + 7));
    }

    volatile uint64_t sink = 0;
    auto fn = [&]()
    {
        sink = extractHash(hashFunction.calculate(row));
    };

    const double totalNs = measure(fn, 3, timedRuns);
    return {hashName, "multikey", numKeys, totalNs, totalNs, 1e3 / totalNs};
}

static void printHeader()
{
    printf("%-12s %-10s %10s %12s %15s\n", "HashFunc", "DataType", "N", "ns/hash", "M hashes/s");
    printf("%-12s %-10s %10s %12s %15s\n", "--------", "--------", "---", "-------", "----------");
}

static void printResult(const BenchResult& r)
{
    printf("%-12s %-10s %10lu %12.2f %15.2f\n", r.name.c_str(), r.dataType.c_str(), r.numValues, r.nsPerHash, r.mHashesPerSec);
}

/// Write results as JSON for the visualization script.
static void writeJson(const std::vector<BenchResult>& results, const char* filename)
{
    FILE* f = fopen(filename, "w");
    if (!f)
    {
        return;
    }
    fprintf(f, "{\n  \"benchmarks\": [\n");
    for (size_t i = 0; i < results.size(); ++i)
    {
        const auto& r = results[i];
        fprintf(f,
                "    {\"name\": \"%s/%s/%lu\", \"real_time\": %.2f, \"cpu_time\": %.2f, "
                "\"iterations\": 1, \"items_per_second\": %.2f, \"bytes_per_second\": 0}%s\n",
                r.name.c_str(),
                r.dataType.c_str(),
                r.numValues,
                r.totalNs,
                r.totalNs,
                r.mHashesPerSec * 1e6,
                (i + 1 < results.size()) ? "," : "");
    }
    fprintf(f, "  ]\n}\n");
    fclose(f);
    printf("\nResults written to %s\n", filename);
}

}

int main()
{
    using namespace NES;

    MurMur3HashFunction murMur3;
    CRC32HashFunction crc32;
    XXH3HashFunction xxh3;
    SipHashFunction sipHash;

    struct HashFuncEntry
    {
        std::string name;
        HashFunction* func;
    };
    std::vector<HashFuncEntry> hashFunctions = {
        {"MurMur3", &murMur3},
        {"CRC32", &crc32},
        {"XXH3", &xxh3},
        {"SipHash", &sipHash},
    };

    const std::vector<uint64_t> sizes = {100, 1000, 10000, 100000, 1000000};
    constexpr int TIMED_RUNS = 5;

    std::vector<BenchResult> allResults;

    /// --- uint64 benchmarks ---
    printf("\n=== uint64 hashing ===\n");
    printHeader();
    for (auto numValues : sizes)
    {
        std::vector<VarVal> values;
        values.reserve(numValues);
        for (uint64_t i = 0; i < numValues; ++i)
        {
            values.emplace_back(nautilus::val<uint64_t>(i * 0xDEADBEEF + 42));
        }

        for (auto& [name, func] : hashFunctions)
        {
            auto r = benchmarkSingleValue(name, *func, "uint64", values, TIMED_RUNS);
            printResult(r);
            allResults.push_back(r);
        }
        printf("\n");
    }

    /// --- int32 benchmarks ---
    printf("=== int32 hashing ===\n");
    printHeader();
    for (auto numValues : sizes)
    {
        std::vector<VarVal> values;
        values.reserve(numValues);
        for (uint64_t i = 0; i < numValues; ++i)
        {
            values.emplace_back(nautilus::val<int32_t>(static_cast<int32_t>(i * 7 + 13)));
        }

        for (auto& [name, func] : hashFunctions)
        {
            auto r = benchmarkSingleValue(name, *func, "int32", values, TIMED_RUNS);
            printResult(r);
            allResults.push_back(r);
        }
        printf("\n");
    }

    /// --- double benchmarks ---
    printf("=== double hashing ===\n");
    printHeader();
    for (auto numValues : sizes)
    {
        std::vector<VarVal> values;
        values.reserve(numValues);
        for (uint64_t i = 0; i < numValues; ++i)
        {
            values.emplace_back(nautilus::val<double>(static_cast<double>(i) * 3.14159));
        }

        for (auto& [name, func] : hashFunctions)
        {
            auto r = benchmarkSingleValue(name, *func, "double", values, TIMED_RUNS);
            printResult(r);
            allResults.push_back(r);
        }
        printf("\n");
    }

    /// --- multi-key benchmarks ---
    printf("=== multi-key hashing (uint64 keys) ===\n");
    printf("%-12s %-10s %10s %12s\n", "HashFunc", "DataType", "NumKeys", "ns/hash");
    printf("%-12s %-10s %10s %12s\n", "--------", "--------", "-------", "-------");
    for (uint64_t numKeys = 1; numKeys <= 8; ++numKeys)
    {
        for (auto& [name, func] : hashFunctions)
        {
            auto r = benchmarkMultiKey(name, *func, numKeys, TIMED_RUNS * 100);
            printf("%-12s %-10s %10lu %12.2f\n", r.name.c_str(), r.dataType.c_str(), r.numValues, r.nsPerHash);
            allResults.push_back(r);
        }
        printf("\n");
    }

    writeJson(allResults, "hash_benchmark_results.json");
    printf("Visualize with: python3 visualize_hash_benchmark.py hash_benchmark_results.json\n");
    return 0;
}
