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

/// Micro benchmark of the state of the hash join: the chained hash map whose entries point to paged vectors.
///
/// The benchmark compiles its build and probe loops with the nautilus MLIR compiler, using the options of the query compiler, and
/// goes through the same *Ref classes as HJBuildPhysicalOperator and HJProbePhysicalOperatorBase (ChainedHashMapRef,
/// ChainedEntryRef, PagedVectorRef and its iterator). Only the surrounding operator machinery (slice store, time functions,
/// execution context, joined record emission) is left out.
///
/// build:  one pass over `records` records whose keys are drawn from `keys` distinct keys: findOrCreateEntry plus pushBack
/// probe:  iterates all entries of the right map, looks each up in the left map and walks both paged vectors
///
/// Arguments: keys, records per thread, payload fields (u64 each), var sized payload bytes (0 = none), hash map buckets
/// Threads: every thread has its own maps and uses the shared buffer manager, as the worker threads of the engine do.

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/ChainedHashMap/EntryRecordsRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <benchmark/benchmark.h>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{

constexpr uint32_t BUFFER_SIZE = 4096; /// operator_buffer_size

struct Workload
{
    uint64_t keys;
    uint32_t payloadFields;
    uint64_t varSizedBytes;
    uint64_t buckets; /// number_of_partitions of the query execution configuration
    uint64_t inlineRecords; /// records of a key that are stored in the entry of the hash map, see number_of_records_per_key

    [[nodiscard]] std::string toString() const
    {
        return std::to_string(keys) + "/" + std::to_string(payloadFields) + "/" + std::to_string(varSizedBytes) + "/"
            + std::to_string(buckets) + "/" + std::to_string(inlineRecords);
    }
};

/// Everything that is shared between the benchmark threads: the buffer manager, the layout and the compiled functions.
struct Shared
{
    Workload workload;
    std::shared_ptr<BufferManager> bufferManager;
    std::shared_ptr<PagedVectorTupleLayout> layout;
    ChainedHashMapConfig config;
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, AbstractBufferProvider*, uint64_t, uint64_t)>> build;
    std::optional<nautilus::engine::CompiledFunction<uint64_t(TupleBuffer*, TupleBuffer*)>> probe;
};

/// The bytes of the var sized payload. Content does not matter, the size does.
const std::array<char, 4096> VAR_SIZED_PAYLOAD = []
{
    std::array<char, 4096> bytes{};
    bytes.fill('x');
    return bytes;
}();

/// The nautilus options of LowerToCompiledQueryPlanPhase::getStage for the compiler mode.
nautilus::engine::NautilusEngine makeProductionEngine()
{
    nautilus::engine::Options options;
    options.setOption("mlir.enableMultithreading", false);
    options.setOption("engine.backend", std::string("mlir"));
    options.setOption("engine.Compilation", true);
    options.setOption("mlir.inline_invoke_calls", true);
    options.setOption("dump.all", false);
    options.setOption("dump.console", false);
    options.setOption("dump.file", false);
    return {options};
}

/// Mixes the record number into a key in [0, keys), so keys are spread randomly over the hash map.
nautilus::val<uint64_t> keyOf(const nautilus::val<uint64_t>& recordNumber, const uint64_t keys)
{
    nautilus::val<uint64_t> x = recordNumber * nautilus::val<uint64_t>(0x9E3779B97F4A7C15ULL);
    x = x ^ (x >> nautilus::val<uint64_t>(32));
    x = x * nautilus::val<uint64_t>(0xD6E8FEB86659FD93ULL);
    x = x ^ (x >> nautilus::val<uint64_t>(32));
    return x % nautilus::val<uint64_t>(keys);
}

Record makeRecord(const Shared& shared, const nautilus::val<uint64_t>& recordNumber)
{
    Record record;
    const auto& names = shared.fieldNames;
    record.write(names[0], VarVal(keyOf(recordNumber, shared.workload.keys)));
    for (size_t field = 1; field <= shared.workload.payloadFields; ++field)
    {
        record.write(names[field], VarVal(recordNumber + nautilus::val<uint64_t>(field)));
    }
    if (shared.workload.varSizedBytes > 0)
    {
        const nautilus::val<int8_t*> content(reinterpret_cast<int8_t*>(const_cast<char*>(VAR_SIZED_PAYLOAD.data())));
        record.write(names.back(), VarVal(VariableSizedData(content, nautilus::val<uint64_t>(shared.workload.varSizedBytes))));
    }
    return record;
}

/// The body of HJBuildPhysicalOperator::execute for one record.
void buildOne(
    const Shared& shared,
    const BorrowedNautilusBuffer& borrowedHashMapBuffer,
    ChainedHashMapRef& hashMap,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider,
    const nautilus::val<uint64_t>& recordNumber)
{
    const auto record = makeRecord(shared, recordNumber);
    const auto& config = shared.config;
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{entry, borrowedHashMapBuffer, config.fieldKeys, config.fieldValues};
            EntryRecordsRef::initialize(
                borrowedHashMapBuffer,
                entryRefReset.getValueMemArea(),
                shared.layout,
                config.inlineRecords,
                config.pageSize,
                bufferProvider);
        },
        bufferProvider);

    const ChainedHashMapRef::ChainedEntryRef entryRef{hashMapEntry, borrowedHashMapBuffer, config.fieldKeys, config.fieldValues};
    EntryRecordsRef records{borrowedHashMapBuffer, entryRef.getValueMemArea(), shared.layout, config.inlineRecords, config.pageSize};
    records.pushBack(record, bufferProvider);
}

Shared& getShared(const Workload& workload)
{
    static std::mutex mutex;
    static std::map<std::string, std::unique_ptr<Shared>> all;
    const std::scoped_lock lock(mutex);
    auto& entry = all[workload.toString()];
    if (entry)
    {
        return *entry;
    }
    entry = std::make_unique<Shared>();
    auto& shared = *entry;
    shared.workload = workload;

    std::vector<DataType> types(1 + workload.payloadFields, DataType{DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE});
    if (workload.varSizedBytes > 0)
    {
        types.emplace_back(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
    }
    const auto schema = TestUtils::createSchemaFromDataTypes(types);
    shared.fieldNames = getOrderedFieldNames(schema);
    shared.layout = std::make_shared<DefaultPagedVectorTupleLayout>(schema);

    /// Same configuration as createChainedHashMapConfig in LowerToPhysicalHashJoin: one key, and a uint32 child index as the value.
    const uint64_t keySize = types[0].getSizeInBytesWithNull();
    const auto inlineRecords = EntryRecordsRef::getEffectiveInlineRecords(schema, workload.inlineRecords, BUFFER_SIZE);
    const auto entrySize = sizeof(ChainedHashMapEntry) + keySize + EntryRecordsRef::getValueAreaSize(inlineRecords, getSizeInBytes(schema));
    const auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(schema, {shared.fieldNames[0]}, {});
    shared.config = ChainedHashMapConfig{
        .entrySize = entrySize,
        .numberOfBuckets = workload.buckets,
        .pageSize = BUFFER_SIZE,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = fieldKeys,
        .fieldValues = fieldValues,
        .hashFunction = std::make_shared<MurMur3HashFunction>(),
        .inlineRecords = inlineRecords};

    constexpr size_t totalMemory = 12ULL << 30;
    shared.bufferManager
        = BufferManager::create(totalMemory, 0.05, BufferAlignment{64}, BUFFER_SIZE, std::make_shared<NesDefaultMemoryAllocator>());

    shared.engine = std::make_unique<nautilus::engine::NautilusEngine>(makeProductionEngine());
    const Shared* sharedPtr = &shared;
    shared.build.emplace(shared.engine->registerFunction(
        std::function(
            [sharedPtr](
                nautilus::val<TupleBuffer*> hashMapBuffer,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<uint64_t> records,
                nautilus::val<uint64_t> firstRecord)
            {
                const auto borrowed = BorrowedNautilusBuffer::from(hashMapBuffer);
                ChainedHashMapRef hashMap{borrowed, sharedPtr->config};
                for (nautilus::val<uint64_t> i = 0; i < records; i = i + 1)
                {
                    buildOne(*sharedPtr, borrowed, hashMap, bufferProvider, firstRecord + i);
                }
            })));

    shared.probe.emplace(shared.engine->registerFunction(
        std::function(
            [sharedPtr](nautilus::val<TupleBuffer*> leftBuffer, nautilus::val<TupleBuffer*> rightBuffer) -> nautilus::val<uint64_t>
            {
                const auto& config = sharedPtr->config;
                const auto leftHashMapBuffer = BorrowedNautilusBuffer::from(leftBuffer);
                const auto rightHashMapBuffer = BorrowedNautilusBuffer::from(rightBuffer);
                ChainedHashMapRef leftHashMap{leftHashMapBuffer, config};
                const ChainedHashMapRef rightHashMap{rightHashMapBuffer, config};
                nautilus::val<uint64_t> checksum = 0;
                const auto& payloadField = sharedPtr->fieldNames[sharedPtr->workload.payloadFields > 0 ? 1 : 0];
                /// The body of HJProbePhysicalOperatorBase::performMatchPairsProbe for one pair of hash maps.
                for (const auto rightEntry : rightHashMap)
                {
                    const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                        rightEntry, rightHashMapBuffer, config.fieldKeys, config.fieldValues};
                    const EntryRecordsRef rightRecords{
                        rightHashMapBuffer, rightEntryRef.getValueMemArea(), sharedPtr->layout, config.inlineRecords, config.pageSize};
                    auto rightItStart = rightRecords.begin();
                    auto rightItEnd = rightRecords.end();

                    if (const auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef); leftEntry != nullptr)
                    {
                        const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                            static_cast<nautilus::val<ChainedHashMapEntry*>>(leftEntry),
                            leftHashMapBuffer,
                            config.fieldKeys,
                            config.fieldValues};
                        const EntryRecordsRef leftRecords{
                            leftHashMapBuffer, leftEntryRef.getValueMemArea(), sharedPtr->layout, config.inlineRecords, config.pageSize};
                        const auto leftItEnd = leftRecords.end();
                        for (auto leftIt = leftRecords.begin(); leftIt != leftItEnd; ++leftIt)
                        {
                            for (auto rightIt = rightItStart; rightIt != rightItEnd; ++rightIt)
                            {
                                const auto leftRecord = *leftIt;
                                const auto rightRecord = *rightIt;
                                checksum = checksum + leftRecord.read(payloadField).getRawValueAs<nautilus::val<uint64_t>>()
                                    + rightRecord.read(payloadField).getRawValueAs<nautilus::val<uint64_t>>();
                            }
                        }
                    }
                }
                return checksum;
            })));
    return shared;
}

/// A hash map of one thread. Its pages and paged vectors are children of its main buffer.
struct HashMap
{
    HashMap(const Shared& shared, AbstractBufferProvider& provider) : shared(shared), provider(provider) { reset(); }

    void reset()
    {
        buffer = TupleBuffer{};
        buffer = getPooledBufferIfFits(
            ChainedHashMap::calculateBufferSize(shared.config.numberOfBuckets, shared.config.bloomFilterMemAreaSize()), provider);
        ChainedHashMap::init(buffer, shared.config);
    }

    const Shared& shared;
    AbstractBufferProvider& provider;
    TupleBuffer buffer;
};

Workload workloadOf(const benchmark::State& state)
{
    return Workload{
        .keys = static_cast<uint64_t>(state.range(0)),
        .payloadFields = static_cast<uint32_t>(state.range(2)),
        .varSizedBytes = static_cast<uint64_t>(state.range(3)),
        .buckets = static_cast<uint64_t>(state.range(4)),
        .inlineRecords = static_cast<uint64_t>(state.range(5))};
}

/// A window of the join: every iteration builds `maps` hash maps (one after the other, each replaces the previous one like the
/// garbage collection of slices does) with `records` records from `keys` keys.
void benchmarkBuild(benchmark::State& state)
{
    auto& shared = getShared(workloadOf(state));
    const auto records = static_cast<uint64_t>(state.range(1));
    const auto maps = static_cast<uint64_t>(state.range(6));
    AbstractBufferProvider& provider = *shared.bufferManager;
    HashMap map(shared, provider);
    uint64_t round = 0;
    for (auto _ : state)
    {
        for (uint64_t window = 0; window < maps; ++window)
        {
            map.reset();
            (*shared.build)(&map.buffer, &provider, records, round++* records);
        }
    }
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(records * maps));
}

/// Probes `maps` pairs of hash maps with `records` records each.
void benchmarkProbe(benchmark::State& state)
{
    auto& shared = getShared(workloadOf(state));
    const auto records = static_cast<uint64_t>(state.range(1));
    const auto maps = static_cast<uint64_t>(state.range(6));
    AbstractBufferProvider& provider = *shared.bufferManager;
    std::vector<std::unique_ptr<HashMap>> left;
    std::vector<std::unique_ptr<HashMap>> right;
    for (uint64_t window = 0; window < maps; ++window)
    {
        left.emplace_back(std::make_unique<HashMap>(shared, provider));
        right.emplace_back(std::make_unique<HashMap>(shared, provider));
        (*shared.build)(&left.back()->buffer, &provider, records, window * 2 * records);
        (*shared.build)(&right.back()->buffer, &provider, records, (window * 2 + 1) * records);
    }
    uint64_t checksum = 0;
    for (auto _ : state)
    {
        for (uint64_t window = 0; window < maps; ++window)
        {
            checksum += (*shared.probe)(&left[window]->buffer, &right[window]->buffer);
        }
    }
    benchmark::DoNotOptimize(checksum);
    /// The checksum is the same for every setup of the data structures, which verifies them.
    state.counters["checksum"] = static_cast<double>(checksum / static_cast<uint64_t>(state.iterations()));
    /// Count what the probe walks: the records of both sides.
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(records * maps * 2));
}

/// keys, records per map, payload fields, var sized bytes, buckets, inline records per key, maps
/// The sizes follow the 10 s windows of Nexmark: a few hundred records with a few dozen keys per window and worker thread.
void arguments(benchmark::internal::Benchmark* benchmark)
{
    for (const int64_t inlineRecords : {0, 2, 4, 8})
    {
        benchmark->Args({50, 250, 3, 0, 128, inlineRecords, 2000});
        benchmark->Args({50, 250, 3, 32, 128, inlineRecords, 2000});
        benchmark->Args({50, 250, 8, 32, 128, inlineRecords, 2000});
        /// more records per key, which spill into pages
        benchmark->Args({10, 2000, 3, 0, 128, inlineRecords, 500});
        benchmark->Args({1000, 1000, 3, 0, 128, inlineRecords, 500});
    }
}

BENCHMARK(benchmarkBuild)->Apply(arguments)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(8)->UseRealTime();
BENCHMARK(benchmarkProbe)->Apply(arguments)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(8)->UseRealTime();

}
}

BENCHMARK_MAIN();
