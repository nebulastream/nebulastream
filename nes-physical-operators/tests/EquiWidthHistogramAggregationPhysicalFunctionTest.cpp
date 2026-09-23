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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <span>
#include <unordered_map>
#include <vector>

#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/Record.hpp>
#include <Operators/Statistic/EquiWidthHistogramBlobLayout.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Statistics/EquiWidthHistogramAggregationPhysicalFunction.hpp>
#include <Statistics/EquiWidthHistogramBlob.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 1024;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

const auto VALUE_FIELD = Identifier::parse("value");
}

/// Drives reset/lift/combine/lower directly and reads the emitted blob back, so the binning decisions are checked
/// where they are made rather than through a whole query.
class EquiWidthHistogramAggregationPhysicalFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("EquiWidthHistogramAggregationPhysicalFunctionTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES,
            UNPOOLED_MEMORY_FRACTION,
            BUFFER_ALIGNMENT,
            POOLED_BUFFER_SIZE,
            std::make_shared<NesDefaultMemoryAllocator>());
    }

    static EquiWidthHistogramAggregationPhysicalFunction
    histogram(const uint64_t numberOfBins, const uint64_t minValue, const uint64_t maxValue)
    {
        const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
        const auto varsized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
        return EquiWidthHistogramAggregationPhysicalFunction{
            uint64Type, varsized, FieldAccessPhysicalFunction{VALUE_FIELD}, VALUE_FIELD, numberOfBins, minValue, maxValue};
    }

    static Record valueRecord(const uint64_t value)
    {
        return Record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{{VALUE_FIELD, VarVal{nautilus::val<uint64_t>{value}}}}};
    }

    /// The counters a histogram holds after seeing `values`, read back out of the blob lower() emits.
    std::vector<uint64_t>
    countersAfter(EquiWidthHistogramAggregationPhysicalFunction& function, const std::initializer_list<uint64_t> values)
    {
        std::vector<int8_t> state(function.getSizeOfStateInBytes(), static_cast<int8_t>(0xFF));
        Arena arena{bufferManager};
        PipelineMemoryProvider memoryProvider{ArenaRef{&arena}, nautilus::val<AbstractBufferProvider*>{bufferManager.get()}};
        const auto statePtr = nautilus::val<AggregationState*>{reinterpret_cast<AggregationState*>(state.data())};

        function.reset(statePtr, nullptr, memoryProvider);
        for (const auto value : values)
        {
            function.lift(statePtr, nullptr, memoryProvider, valueRecord(value));
        }
        return binsOf(function.lower(statePtr, nullptr, memoryProvider));
    }

    /// The blob a lowered record carries, validated and decoded into plain counters.
    static std::vector<uint64_t> binsOf(const Record& lowered)
    {
        const auto blob = lowered.read(VALUE_FIELD).getRawValueAs<VariableSizedData>();
        /// Outside a trace a nautilus value is just the value, which is what lets this read the blob as plain bytes.
        const auto* const data = nautilus::details::RawValueResolver<int8_t*>::getRawValue(blob.getContent());
        const auto size = nautilus::details::RawValueResolver<uint64_t>::getRawValue(blob.getSize());
        validateEquiWidthHistogramBlob(std::span<const int8_t>{data, size});

        std::vector<uint64_t> counters((size - EquiWidthHistogramBlob::HEADER_SIZE) / EquiWidthHistogramBlob::COUNTER_SIZE);
        std::memcpy(counters.data(), data + EquiWidthHistogramBlob::HEADER_SIZE, counters.size() * EquiWidthHistogramBlob::COUNTER_SIZE);
        return counters;
    }

    std::shared_ptr<AbstractBufferProvider> bufferManager;
};

/// reset zeroes the counters, so a state reused from a previous window starts empty.
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, resetClearsEveryCounter)
{
    auto function = histogram(5, 0, 25);
    EXPECT_EQ(function.getSizeOfStateInBytes(), 5 * sizeof(uint64_t));
    EXPECT_EQ(countersAfter(function, {}), (std::vector<uint64_t>{0, 0, 0, 0, 0}));
}

/// w = (25 - 0) / 5 = 5, so the bins are [0,5), [5,10), [10,15), [15,20) and [20,25].
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, valuesLandInTheBinTheirRangeSaysTheyDo)
{
    auto function = histogram(5, 0, 25);
    /// 0 is the first value of the first bin, 4 its last, 5 the first of the second, 24 the last regular value.
    EXPECT_EQ(countersAfter(function, {0, 4, 5, 9, 10, 24}), (std::vector<uint64_t>{2, 2, 1, 0, 1}));
}

/// maxValue belongs to the last bin, and so does everything outside the range on either side. Documented on the
/// class, tested here so that changing it is a visible decision rather than a silent one.
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, outOfRangeValuesAreCountedInTheLastBin)
{
    auto function = histogram(5, 10, 35);
    /// Below the minimum, the maximum itself, and above the maximum -- one of each, all in the last bin.
    EXPECT_EQ(countersAfter(function, {0, 9, 35, 36, 1000000}), (std::vector<uint64_t>{0, 0, 0, 0, 5}));
    /// A value in range still lands where it belongs, so the last bin is not simply swallowing everything.
    EXPECT_EQ(countersAfter(function, {10, 0}), (std::vector<uint64_t>{1, 0, 0, 0, 1}));
}

/// An integer width leaves a remainder, which widens the last bin rather than shifting the others.
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, aRangeWithARemainderWidensTheLastBin)
{
    /// w = (25 - 0) / 4 = 6: [0,6), [6,12), [12,18) and [18,25], the last one two values wider.
    auto function = histogram(4, 0, 25);
    EXPECT_EQ(countersAfter(function, {5, 6, 17, 18, 23, 24, 25}), (std::vector<uint64_t>{1, 1, 1, 4}));
}

/// Windows are aggregated per worker thread and merged, so combine has to add the counters bin by bin.
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, combineAddsTheCountersOfBothStates)
{
    auto function = histogram(5, 0, 25);
    std::vector<int8_t> state1(function.getSizeOfStateInBytes(), 0);
    std::vector<int8_t> state2(function.getSizeOfStateInBytes(), 0);
    Arena arena{bufferManager};
    PipelineMemoryProvider memoryProvider{ArenaRef{&arena}, nautilus::val<AbstractBufferProvider*>{bufferManager.get()}};
    const auto statePtr1 = nautilus::val<AggregationState*>{reinterpret_cast<AggregationState*>(state1.data())};
    const auto statePtr2 = nautilus::val<AggregationState*>{reinterpret_cast<AggregationState*>(state2.data())};

    function.reset(statePtr1, nullptr, memoryProvider);
    function.reset(statePtr2, nullptr, memoryProvider);
    for (const uint64_t value : {0U, 0U, 7U})
    {
        function.lift(statePtr1, nullptr, memoryProvider, valueRecord(value));
    }
    for (const uint64_t value : {7U, 23U})
    {
        function.lift(statePtr2, nullptr, memoryProvider, valueRecord(value));
    }

    function.combine(statePtr1, nullptr, statePtr2, nullptr, memoryProvider);

    EXPECT_EQ(binsOf(function.lower(statePtr1, nullptr, memoryProvider)), (std::vector<uint64_t>{2, 2, 0, 0, 1}));
    /// The right-hand state is left alone, as the caller still owns it.
    EXPECT_EQ(binsOf(function.lower(statePtr2, nullptr, memoryProvider)), (std::vector<uint64_t>{0, 1, 0, 0, 1}));
}

/// The blob carries the geometry a probe needs to turn counters back into bins.
TEST_F(EquiWidthHistogramAggregationPhysicalFunctionTest, theLoweredBlobCarriesTheHeader)
{
    auto function = histogram(12, 7, 99);
    std::vector<int8_t> state(function.getSizeOfStateInBytes(), 0);
    Arena arena{bufferManager};
    PipelineMemoryProvider memoryProvider{ArenaRef{&arena}, nautilus::val<AbstractBufferProvider*>{bufferManager.get()}};
    const auto statePtr = nautilus::val<AggregationState*>{reinterpret_cast<AggregationState*>(state.data())};

    function.reset(statePtr, nullptr, memoryProvider);
    function.lift(statePtr, nullptr, memoryProvider, valueRecord(50));

    const auto blob = function.lower(statePtr, nullptr, memoryProvider).read(VALUE_FIELD).getRawValueAs<VariableSizedData>();
    const auto blobMemory = static_cast<nautilus::val<int8_t*>>(blob.getContent());
    EXPECT_EQ(blob.getSize(), EquiWidthHistogramBlob::HEADER_SIZE + (12 * EquiWidthHistogramBlob::COUNTER_SIZE));
    EXPECT_EQ(readEquiWidthHistogramBlobNumberOfBins(blobMemory), 12U);
    EXPECT_EQ(readEquiWidthHistogramBlobMinValue(blobMemory), 7U);
    EXPECT_EQ(readEquiWidthHistogramBlobMaxValue(blobMemory), 99U);

    /// w = (99 - 7) / 12 = 7, so 50 is in bin (50 - 7) / 7 = 6, which spans [49, 56).
    const EquiWidthHistogramBlobBinReader reader{blobMemory};
    EXPECT_EQ(reader.getBinCounter(nautilus::val<uint64_t>{6}), 1U);
    EXPECT_EQ(reader.getBinStart(nautilus::val<uint64_t>{6}), 49U);
    EXPECT_EQ(reader.getBinEnd(nautilus::val<uint64_t>{6}), 56U);
}

}
