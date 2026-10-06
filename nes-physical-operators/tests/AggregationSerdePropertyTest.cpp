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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <tuple>
#include <utility>
#include <vector>
#include <rapidcheck.h>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/CountAggregationPhysicalFunction.hpp>
#include <Aggregation/Function/SumAggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <gtest/gtest.h>
#include <rapidcheck/gtest.h>
#include <HashMapSlice.hpp>
#include "CompiledSerdeTestHarness.hpp"

namespace NES
{
namespace
{
using Entry = std::pair<uint64_t, std::vector<uint8_t>>;
constexpr OperatorHandlerId HANDLER_ID{0};
constexpr std::array KEY_TYPES = {DataType::Type::UINT8, DataType::Type::UINT16, DataType::Type::UINT32, DataType::Type::UINT64};

struct Layout
{
    ChainedHashMapConfig config;
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> functions;
};

Layout makeLayout(std::mt19937_64& random, const size_t keyCount, const size_t aggregationCount)
{
    Layout result;
    uint64_t offset = sizeof(ChainedHashMapEntry);
    for (size_t index = 0; index < keyCount; ++index)
    {
        const DataType type(KEY_TYPES.at(random() % KEY_TYPES.size()), DataType::NULLABLE::NOT_NULLABLE);
        result.config.fieldKeys.push_back({QualifiedIdentifier::parse("key" + std::to_string(index)), type, offset});
        offset += type.getSizeInBytesWithNull();
    }
    for (size_t index = 0; index < aggregationCount; ++index)
    {
        const auto name = QualifiedIdentifier::parse("aggregate" + std::to_string(index));
        const auto input = PhysicalFunction(FieldAccessPhysicalFunction(QualifiedIdentifier::parse("input")));
        const DataType resultType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
        const bool sum = random() % 2 == 0;
        const DataType inputType(
            random() % 2 == 0 ? DataType::Type::UINT32 : DataType::Type::UINT64,
            sum && random() % 2 == 0 ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
        std::shared_ptr<AggregationPhysicalFunction> function;
        if (sum)
        {
            function = std::make_shared<SumAggregationPhysicalFunction>(inputType, resultType, input, name);
        }
        else
        {
            function = std::make_shared<CountAggregationPhysicalFunction>(inputType, resultType, input, name, true);
        }
        result.config.fieldValues.push_back({name, resultType, offset});
        offset += function->getSizeOfStateInBytes();
        result.functions.push_back(std::move(function));
    }
    result.config.entrySize = offset;
    result.config.numberOfBuckets = 32;
    result.config.pageSize = 128;
    result.config.hashFunction = std::make_shared<MurMur3HashFunction>();
    return result;
}

std::shared_ptr<AggregationOperatorHandler> makeHandler()
{
    return std::make_shared<AggregationOperatorHandler>(
        std::vector{OriginId(1)}, OriginId(2), std::make_unique<DefaultTimeBasedSliceStore>(100, 100, SliceCacheConfiguration{}));
}

std::vector<Entry> mapEntries(const TupleBuffer* buffer, const size_t entrySize)
{
    std::vector<Entry> result;
    if (buffer == nullptr)
    {
        return result;
    }
    const auto map = ChainedHashMap::load(*buffer);
    for (uint64_t page = 0; page < map.getNumberOfPages(); ++page)
    {
        auto data = map.getPage(page);
        const auto* memory = data.getAvailableMemoryArea().data();
        for (uint64_t record = 0; record < data.getNumberOfTuples(); ++record)
        {
            const auto* entry = reinterpret_cast<const ChainedHashMapEntry*>(memory + record * entrySize);
            const auto* payload = reinterpret_cast<const uint8_t*>(entry + 1);
            result.emplace_back(entry->hash, std::vector<uint8_t>(payload, payload + entrySize - sizeof(ChainedHashMapEntry)));
        }
    }
    std::ranges::sort(result);
    return result;
}

void populate(
    AggregationOperatorHandler& handler,
    Testing::SerdePipelineContext& context,
    const ChainedHashMapConfig& config,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& functions,
    std::mt19937_64& random,
    const uint64_t sliceCount)
{
    auto& store = dynamic_cast<DefaultTimeBasedSliceStore&>(handler.getSliceAndWindowStore());
    const CreateNewHashMapSliceArgs args(config, *context.buffers);
    std::vector<std::shared_ptr<Slice>> slices;
    std::vector<DefaultTimeBasedSliceStore::WindowSnapshot> windows;
    for (uint64_t index = 0; index < sliceCount; ++index)
    {
        const auto start = index * 100;
        auto slice = std::make_shared<AggregationSlice>(SliceStart(start), SliceEnd(start + 100), args, context.workers);
        for (uint64_t worker = 0; worker < context.workers; ++worker)
        {
            if (not(index == 0 && worker == 0) && random() % 4 == 0)
            {
                continue; // An unallocated worker map must stay unallocated.
            }
            auto map = ChainedHashMap::load(*slice->getOrCreateHashMapBufferRefForWorker(*context.buffers, WorkerThreadId(worker)));
            for (uint64_t key = 0; key < 8; ++key)
            {
                if (not(index == 0 && worker == 0) && random() % 3 == 0)
                {
                    continue;
                }
                const uint64_t hash = 1; // Distinct keys also share a hash, exercising full-key equality.
                auto* entry = map.insertEntry(
                    hash,
                    context.buffers.get(),
                    config.entrySize,
                    config.entriesPerPage(),
                    config.pageSize,
                    ChainedHashMap::calculateMask(config.numberOfBuckets));
                auto* payload = reinterpret_cast<uint8_t*>(entry) + sizeof(ChainedHashMapEntry);
                std::memset(payload, 0, config.entrySize - sizeof(ChainedHashMapEntry));
                for (const auto& field : config.fieldKeys)
                {
                    const auto value = key;
                    std::memcpy(payload + field.fieldOffset - sizeof(ChainedHashMapEntry), &value, field.type.getSizeInBytesWithNull());
                }
                for (size_t aggregate = 0; aggregate < functions.size(); ++aggregate)
                {
                    const auto& field = config.fieldValues[aggregate];
                    const auto nullableSum = functions[aggregate]->getSizeOfStateInBytes() == sizeof(uint64_t) + 1;
                    if (nullableSum)
                    {
                        payload[field.fieldOffset - sizeof(ChainedHashMapEntry)] = random() % 2;
                    }
                    const auto value
                        = nullableSum && payload[field.fieldOffset - sizeof(ChainedHashMapEntry)] ? uint64_t{0} : random() % 100;
                    std::memcpy(payload + field.fieldOffset - sizeof(ChainedHashMapEntry) + (nullableSum ? 1 : 0), &value, sizeof(value));
                }
            }
        }
        slices.push_back(slice);
        windows.push_back(
            {WindowInfo(start, start + 100), random() % 2 == 0 ? WindowInfoState::WINDOW_FILLING : WindowInfoState::EMITTED_TO_PROBE});
    }
    store.restore(std::move(slices), windows, random() % 1000 + 1);
    auto watermark = handler.snapshotWatermarkState();
    watermark.build[0].sequence = random() % 100;
    watermark.build[0].watermark = random() % 1000;
    watermark.probe[0].sequence = random() % 100;
    watermark.probe[0].watermark = random() % 1000;
    watermark.lastForwarded = random() % 1000;
    handler.restoreWatermarkState(watermark);
}

std::vector<Entry> combinedEntries(
    const AggregationSlice& slice,
    const ChainedHashMapConfig& config,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& functions)
{
    std::map<std::vector<uint8_t>, Entry> byKey;
    const auto keyBytes = config.fieldValues.front().fieldOffset - sizeof(ChainedHashMapEntry);
    for (uint64_t worker = 0; worker < slice.getNumberOfHashMaps(); ++worker)
    {
        for (auto [hash, payload] : mapEntries(slice.getHashMapBufferRefForWorker(WorkerThreadId(worker)), config.entrySize))
        {
            const std::vector<uint8_t> key(payload.begin(), payload.begin() + keyBytes);
            const auto [position, inserted] = byKey.try_emplace(key, hash, payload);
            if (inserted)
            {
                continue;
            }
            RC_ASSERT(position->second.first == hash);
            auto& destination = position->second.second;
            for (size_t index = 0; index < functions.size(); ++index)
            {
                const auto offset = config.fieldValues[index].fieldOffset - sizeof(ChainedHashMapEntry);
                const bool nullable = functions[index]->getSizeOfStateInBytes() == sizeof(uint64_t) + 1;
                if (nullable)
                {
                    destination[offset] = destination[offset] && payload[offset];
                }
                uint64_t existing = 0;
                uint64_t incoming = 0;
                std::memcpy(&existing, destination.data() + offset + nullable, sizeof(existing));
                std::memcpy(&incoming, payload.data() + offset + nullable, sizeof(incoming));
                existing += incoming;
                std::memcpy(destination.data() + offset + nullable, &existing, sizeof(existing));
            }
        }
    }
    std::vector<Entry> result;
    for (auto& [key, entry] : byKey)
    {
        result.push_back(std::move(entry));
    }
    std::ranges::sort(result);
    return result;
}

void assertEqualState(
    const AggregationOperatorHandler& donor,
    const AggregationOperatorHandler& recipient,
    const ChainedHashMapConfig& config,
    const std::vector<std::shared_ptr<AggregationPhysicalFunction>>& functions)
{
    const auto donorWatermarks = donor.snapshotWatermarkState();
    const auto recipientWatermarks = recipient.snapshotWatermarkState();
    RC_ASSERT(donorWatermarks.build.size() == recipientWatermarks.build.size());
    RC_ASSERT(donorWatermarks.probe.size() == recipientWatermarks.probe.size());
    RC_ASSERT(donorWatermarks.build[0].sequence == recipientWatermarks.build[0].sequence);
    RC_ASSERT(donorWatermarks.build[0].watermark == recipientWatermarks.build[0].watermark);
    RC_ASSERT(donorWatermarks.probe[0].sequence == recipientWatermarks.probe[0].sequence);
    RC_ASSERT(donorWatermarks.probe[0].watermark == recipientWatermarks.probe[0].watermark);
    RC_ASSERT(donorWatermarks.lastForwarded == recipientWatermarks.lastForwarded);

    const auto& oldStore = dynamic_cast<const DefaultTimeBasedSliceStore&>(donor.getSliceAndWindowStore());
    const auto& newStore = dynamic_cast<const DefaultTimeBasedSliceStore&>(recipient.getSliceAndWindowStore());
    const auto oldState = oldStore.snapshot();
    const auto newState = newStore.snapshot();
    RC_ASSERT(oldState.nextSequence == newState.nextSequence);
    RC_ASSERT(oldState.slices.size() == newState.slices.size());
    RC_ASSERT(oldState.windows.size() == newState.windows.size());
    for (size_t index = 0; index < oldState.windows.size(); ++index)
    {
        RC_ASSERT(oldState.windows[index].info.windowStart == newState.windows[index].info.windowStart);
        RC_ASSERT(oldState.windows[index].info.windowEnd == newState.windows[index].info.windowEnd);
        RC_ASSERT(oldState.windows[index].state == newState.windows[index].state);
    }
    for (size_t index = 0; index < oldState.slices.size(); ++index)
    {
        const auto& oldSlice = dynamic_cast<const AggregationSlice&>(*oldState.slices[index]);
        const auto& newSlice = dynamic_cast<const AggregationSlice&>(*newState.slices[index]);
        RC_ASSERT(oldSlice.getSliceStart() == newSlice.getSliceStart());
        RC_ASSERT(oldSlice.getSliceEnd() == newSlice.getSliceEnd());
        RC_ASSERT(combinedEntries(oldSlice, config, functions) == combinedEntries(newSlice, config, functions));
        for (uint64_t worker = 1; worker < newSlice.getNumberOfHashMaps(); ++worker)
        {
            RC_ASSERT(newSlice.getHashMapBufferRefForWorker(WorkerThreadId(worker)) == nullptr);
        }
    }
}

void roundTrip(const size_t keys, const size_t aggregations, const uint64_t seed)
{
    std::mt19937_64 random(seed);
    auto layout = makeLayout(random, keys, aggregations);
    Testing::SerdePipelineContext donorContext(random() % 3 + 1);
    Testing::SerdePipelineContext recipientContext(random() % 3 + 1);
    auto donor = makeHandler();
    auto recipient = makeHandler();
    donorContext.handlers.emplace(HANDLER_ID, donor);
    recipientContext.handlers.emplace(HANDLER_ID, recipient);
    const auto sliceCount = random() % 3 + 1;
    populate(*donor, donorContext, layout.config, layout.functions, random, sliceCount);
    const auto initialSnapshot = dynamic_cast<const DefaultTimeBasedSliceStore&>(donor->getSliceAndWindowStore()).snapshot();
    const auto& firstSlice = dynamic_cast<const AggregationSlice&>(*initialSnapshot.slices.front());
    RC_ASSERT(ChainedHashMap::load(*firstSlice.getHashMapBufferRefForWorker(WorkerThreadId(0))).getNumberOfPages() > 1);
    AggregationBuildPhysicalOperator operation(
        HANDLER_ID,
        nullptr,
        nullptr,
        layout.functions,
        layout.config,
        {},
        AggregationBuildPhysicalOperator::DonorLayout{layout.config.entrySize, layout.functions});
    Testing::CompiledSerdeTestHarness::compile([&](CompilationContext& context) { operation.registerMigrationFunctions(context); });
    const auto state = Testing::CompiledSerdeTestHarness::emit(operation, donorContext);
    const auto donorSnapshot = dynamic_cast<const DefaultTimeBasedSliceStore&>(donor->getSliceAndWindowStore()).snapshot();
    const auto watermarkSnapshot = donor->snapshotWatermarkState();
    uint64_t expectedBytes = 8 * (4 + 2 * (watermarkSnapshot.build.size() + watermarkSnapshot.probe.size()));
    expectedBytes += 3 * 8 * donorSnapshot.slices.size() + 3 * 8 * donorSnapshot.windows.size();
    for (const auto& slice : donorSnapshot.slices)
    {
        expectedBytes += combinedEntries(dynamic_cast<const AggregationSlice&>(*slice), layout.config, layout.functions).size()
            * (8 + layout.config.entrySize - sizeof(ChainedHashMapEntry));
    }
    const auto childBuffer = PipelineStateReader(state).child(0);
    RC_ASSERT(childBuffer.getBufferSize() == (expectedBytes + 63) / 64 * 64);
    const auto bytes = childBuffer.getAvailableMemoryArea();
    RC_ASSERT(std::all_of(bytes.begin() + expectedBytes, bytes.end(), [](std::byte byte) { return byte == std::byte{0}; }));
    Testing::CompiledSerdeTestHarness::absorb(operation, state, recipientContext);
    assertEqualState(*donor, *recipient, layout.config, layout.functions);
}
}

RC_GTEST_PROP(AggregationSerdePropertyTest, compiledEmitAbsorbPreservesRandomLayoutsAndStates, ())
{
    const auto keys = *rc::gen::inRange<size_t>(1, 4);
    const auto aggregations = *rc::gen::inRange<size_t>(1, 4);
    const auto seed = *rc::gen::arbitrary<uint64_t>();
    roundTrip(keys, aggregations, seed);
}
}
